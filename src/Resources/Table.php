<?php

namespace DreamFactory\Core\SqlDb\Resources;

use Config;
use DreamFactory\Core\Database\Command;
use DreamFactory\Core\Database\RelationSchema;
use DreamFactory\Core\Database\Transaction;
use DreamFactory\Core\Database\Expression;
use DreamFactory\Core\Database\ColumnSchema;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\NotImplementedException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Resources\BaseDbTableResource;
use DreamFactory\Core\SqlDb\Components\SqlDbResource;
use DreamFactory\Core\SqlDb\Components\TableDescriber;
use DreamFactory\Core\Utility\DbUtilities;
use DreamFactory\Core\Utility\ResourcesWrapper;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Library\Utility\Enums\Verbs;

/**
 * Class Table
 *
 * @package DreamFactory\Core\SqlDb\Resources
 */
class Table extends BaseDbTableResource
{
    //*************************************************************************
    //	Traits
    //*************************************************************************

    use SqlDbResource;
    use TableDescriber;

    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var null | Transaction
     */
    protected $transaction = null;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * {@InheritDoc}
     */
    protected function detectRequestMembers()
    {
        parent::detectRequestMembers();

        if (!empty($this->resource)) {
            // All calls can request related data to be returned
            $related = ArrayUtils::get($this->options, ApiOptions::RELATED);
            if (!empty($related) && is_string($related) && ('*' !== $related)) {
                $relations = [];
                if (!is_array($related)) {
                    $related = array_map('trim', explode(',', $related));
                }
                foreach ($related as $relative) {
                    $extraFields = ArrayUtils::get($this->options, $relative . '_fields', '*');
                    $extraOrder = ArrayUtils::get($this->options, $relative . '_order', '');
                    $relations[] =
                        [
                            'name'             => $relative,
                            ApiOptions::FIELDS => $extraFields,
                            ApiOptions::ORDER  => $extraOrder
                        ];
                }

                $this->options['related'] = $relations;
            }
        }

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function updateRecordsByFilter($table, $record, $filter = null, $params = [], $extras = [])
    {
        $record = DbUtilities::validateAsArray($record, null, false, 'There are no fields in the record.');

        $idFields = ArrayUtils::get($extras, ApiOptions::ID_FIELD);
        $idTypes = ArrayUtils::get($extras, ApiOptions::ID_TYPE);
        $fields = ArrayUtils::get($extras, ApiOptions::FIELDS);
        $related = ArrayUtils::get($extras, ApiOptions::RELATED);
        $allowRelatedDelete = ArrayUtils::getBool($extras, ApiOptions::ALLOW_RELATED_DELETE, false);
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');

        try {
            $fieldsInfo = $this->getFieldsInfo($table);
            $idsInfo = $this->getIdsInfo($table, $fieldsInfo, $idFields, $idTypes);
            $relatedInfo = $this->describeTableRelated($table);
            $fields = (empty($fields)) ? $idFields : $fields;
            $result = $this->parseFieldsForSqlSelect($fields, $fieldsInfo);
            $bindings = ArrayUtils::get($result, 'bindings');
            $fields = ArrayUtils::get($result, 'fields');
            $fields = (empty($fields)) ? '*' : $fields;

            $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters, true);

            // build filter string if necessary, add server-side filters if necessary
            $criteria = $this->convertFilterToNative($filter, $params, $ssFilters, $fieldsInfo);
            $where = ArrayUtils::get($criteria, 'where');
            $params = ArrayUtils::get($criteria, 'params', []);

            if (!empty($parsed)) {
                $this->dbConn->createCommand()->update($table, $parsed, $where, $params);
            }

            $results = $this->recordQuery($table, $fields, $where, $params, $bindings, $extras);

            if (!empty($relatedInfo)) {
                // update related info
                foreach ($results as $row) {
                    $id = static::checkForIds($row, $idsInfo, $extras);
                    $this->updateRelations($table, $record, $id, $relatedInfo, $allowRelatedDelete);
                }
                // get latest with related changes if requested
                if (!empty($related)) {
                    $results = $this->recordQuery($table, $fields, $where, $params, $bindings, $extras);
                }
            }

            return $results;
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to update records in '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function patchRecordsByFilter($table, $record, $filter = null, $params = [], $extras = [])
    {
        // currently the same as update here
        return $this->updateRecordsByFilter($table, $record, $filter, $params, $extras);
    }

    /**
     * {@inheritdoc}
     */
    public function truncateTable($table, $extras = [])
    {
        // truncate the table, return success
        try {
            // build filter string if necessary, add server-side filters if necessary
            $ssFilters = ArrayUtils::get($extras, 'ss_filters');
            $params = [];
            $serverFilter = $this->buildQueryStringFromData($ssFilters, $params);
            if (!empty($serverFilter)) {
                $this->dbConn->createCommand()->delete($table, $serverFilter, $params);
            } else {
                $this->dbConn->createCommand()->truncateTable($table);
            }

            return ['success' => true];
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function deleteRecordsByFilter($table, $filter, $params = [], $extras = [])
    {
        if (empty($filter)) {
            throw new BadRequestException("Filter for delete request can not be empty.");
        }

        $idFields = ArrayUtils::get($extras, ApiOptions::ID_FIELD);
        $idTypes = ArrayUtils::get($extras, ApiOptions::ID_TYPE);
        $fields = ArrayUtils::get($extras, ApiOptions::FIELDS);
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');

        try {
            $fieldsInfo = $this->getFieldsInfo($table);
            /*$idsInfo = */
            $this->getIdsInfo($table, $fieldsInfo, $idFields, $idTypes);
            $fields = (empty($fields)) ? $idFields : $fields;
            $result = $this->parseFieldsForSqlSelect($fields, $fieldsInfo);
            $bindings = ArrayUtils::get($result, 'bindings');
            $fields = ArrayUtils::get($result, 'fields');
            $fields = (empty($fields)) ? '*' : $fields;

            // build filter string if necessary, add server-side filters if necessary
            $criteria = $this->convertFilterToNative($filter, $params, $ssFilters, $fieldsInfo);
            $where = ArrayUtils::get($criteria, 'where');
            $params = ArrayUtils::get($criteria, 'params', []);

            $results = $this->recordQuery($table, $fields, $where, $params, $bindings, $extras);
            $this->dbConn->createCommand()->delete($table, $where, $params);

            return $results;
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        $fields = ArrayUtils::get($extras, ApiOptions::FIELDS);
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');

        try {
            $fieldsInfo = $this->getFieldsInfo($table);
            $result = $this->parseFieldsForSqlSelect($fields, $fieldsInfo);
            $bindings = ArrayUtils::get($result, 'bindings');
            $fields = ArrayUtils::get($result, 'fields');
            $fields = (empty($fields)) ? '*' : $fields;

            // build filter string if necessary, add server-side filters if necessary
            $criteria = $this->convertFilterToNative($filter, $params, $ssFilters, $fieldsInfo);
            $where = ArrayUtils::get($criteria, 'where');
            $params = ArrayUtils::get($criteria, 'params', []);

            return $this->recordQuery($table, $fields, $where, $params, $bindings, $extras);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to retrieve records from '$table'.\n{$ex->getMessage()}");
        }
    }

    // Helper methods

    /**
     * @param $table
     * @param $select
     * @param $where
     * @param $bind_values
     * @param $bind_columns
     * @param $extras
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     * @throws \DreamFactory\Core\Exceptions\NotFoundException
     * @throws \DreamFactory\Core\Exceptions\RestException
     * @throws \Exception
     */
    protected function recordQuery($table, $select, $where, $bind_values, $bind_columns, $extras)
    {
        $order = ArrayUtils::get($extras, ApiOptions::ORDER);
        $limit = intval(ArrayUtils::get($extras, ApiOptions::LIMIT, 0));
        $offset = intval(ArrayUtils::get($extras, ApiOptions::OFFSET, 0));
        $maxAllowed = static::getMaxRecordsReturnedLimit();
        $needLimit = false;

        $from = $table;

        // use query builder
//        $builder = $this->dbConn->getSchema()->getCommandBuilder();
//        $criteria = $builder->createCriteria();
//        $criteria->select = $select;
//        if (!empty($where)) {
//            $criteria->addCondition($where);
//        }
//        if (!empty($order)) {
//            $criteria->order = ($order);
//        }
//        if (($limit < 1) || ($limit > $maxAllowed)) {
//            // impose a limit to protect server
//            $limit = $maxAllowed;
//            $needLimit = true;
//        }
//        $criteria->limit = $limit;
//        $criteria->offset = $offset;
//        $command = $builder->createFindCommand($table, $criteria);

        /** @var Command $command */
        $command = $this->dbConn->createCommand();
        $command->select($select);

        $command->from($from);

        if (!empty($where)) {
            $command->where($where);
        }
        if (!empty($order)) {
            $command->order($order);
        }
        if (($limit < 1) || ($limit > $maxAllowed)) {
            // impose a limit to protect server
            $limit = $maxAllowed;
            $needLimit = true;
        }
        $command->limit($limit, $offset);

        // must bind values after setting all components of the query,
        // otherwise yii text string is generated without limit, etc.
        if (!empty($bind_values)) {
            $command->bindValues($bind_values);
        }

        $reader = $command->query();
        $data = [];
        $dummy = [];
        foreach ($bind_columns as $binding) {
            $name = ArrayUtils::get($binding, 'name');
            $type = ArrayUtils::get($binding, 'pdo_type');
            $reader->bindColumn($name, $dummy[$name], $type);
        }
        $reader->setFetchMode(\PDO::FETCH_BOUND);
        $row = 0;
        while (false !== $read = $reader->read()) {
            $temp = [];
            foreach ($bind_columns as $binding) {
                $name = ArrayUtils::get($binding, 'name');
                $type = ArrayUtils::get($binding, 'php_type');
                if (null === $value = ArrayUtils::get($dummy, $name)) {
                    $value = (is_array($read) ? ArrayUtils::get($read, $name) : null);
                }
                if (!is_null($value)) {
                    $value = $this->dbConn->getSchema()->formatValue($value, $type);
                }
                $temp[$name] = $value;
            }

            $data[$row++] = $temp;
        }

        $meta = [];
        $includeCount = ArrayUtils::getBool($extras, ApiOptions::INCLUDE_COUNT, false);
        // count total records
        if ($includeCount || $needLimit) {
            $command->reset();
            $command->select('(COUNT(*)) as ' . $this->dbConn->quoteColumnName('count'));
            $command->from($from);
            if (!empty($where)) {
                $command->where($where);
            }
            if (!empty($bind_values)) {
                $command->bindValues($bind_values);
            }

            $count = intval($command->queryScalar());

            if ($includeCount || $count > $maxAllowed) {
                $meta['count'] = $count;
            }
            if (($count - $offset) > $limit) {
                $meta['next'] = $offset + $limit;
            }
        }

        if (ArrayUtils::getBool($extras, ApiOptions::INCLUDE_SCHEMA, false)) {
            try {
                $schema = $this->dbConn->getSchema()->getTable($table);
                if (!$schema) {
                    throw new NotFoundException("Table '$table' does not exist in the database.");
                }

                $meta['schema'] = $schema->toArray(true);
            } catch (RestException $ex) {
                throw $ex;
            } catch (\Exception $ex) {
                throw new InternalServerErrorException("Error describing database table '$table'.\n" .
                    $ex->getMessage(), $ex->getCode());
            }
        }

        $related = ArrayUtils::get($extras, ApiOptions::RELATED);
        if (!empty($related)) {
            $relations = $this->describeTableRelated($table);
            foreach ($data as $key => $temp) {
                $data[$key] = $this->retrieveRelatedRecords($temp, $relations, $related);
            }
        }

        if (!empty($meta)) {
            $data['meta'] = $meta;
        }

        return $data;
    }

    /**
     * @param $table_name
     *
     * @return array
     * @throws \Exception
     */
    protected function getFieldsInfo($table_name)
    {
        $table = $this->dbConn->getSchema()->getTable($table_name);
        if (!$table) {
            throw new NotFoundException("Table '$table_name' does not exist in the database.");
        }

        // re-index for alias usage, easier to find requested fields from client
        $columns = [];
        /** @var ColumnSchema $column */
        foreach ($table->columns as $column) {
            $columns[strtolower($column->getName(true))] = $column;
        }

        return $columns;
    }

    /**
     * @param $table_name
     *
     * @return array
     * @throws \Exception
     */
    protected function describeTableRelated($table_name)
    {
        $table = $this->dbConn->getSchema()->getTable($table_name);
        if (!$table) {
            throw new NotFoundException("Table '$table_name' does not exist in the database.");
        }

        $relatives = [];
        /** @var RelationSchema $relation */
        foreach ($table->relations as $relation) {
            $relatives[$relation->name] = $relation->toArray();
        }

        return $relatives;
    }

    /**
     * Take in a ANSI SQL filter string (WHERE clause)
     * or our generic NoSQL filter array or partial record
     * and parse it to the service's native filter criteria.
     * The filter string can have substitution parameters such as
     * ':name', in which case an associative array is expected,
     * for value substitution.
     *
     * @param string | array $filter       SQL WHERE clause filter string
     * @param array          $params       Array of substitution values
     * @param array          $ss_filters   Server-side filters to apply
     * @param array          $avail_fields All available fields for the table
     *
     * @throws BadRequestException
     * @return mixed
     */
    protected function convertFilterToNative($filter, $params = [], $ss_filters = [], $avail_fields = [])
    {
        // interpret any parameter values as lookups
        $params = ArrayUtils::clean(static::interpretRecordValues($params));

        if (!is_array($filter)) {
            Session::replaceLookups($filter);
            $filterString = '';
            $clientFilter = $this->parseFilterString($filter, $params, $avail_fields);
            if (!empty($clientFilter)) {
                $filterString = $clientFilter;
            }
            $serverFilter = $this->buildQueryStringFromData($ss_filters, $params);
            if (!empty($serverFilter)) {
                if (empty($filterString)) {
                    $filterString = $serverFilter;
                } else {
                    $filterString = '(' . $filterString . ') AND (' . $serverFilter . ')';
                }
            }

            return ['where' => $filterString, 'params' => $params];
        } else {
            // todo parse client filter?
            $filterArray = $filter;
            $serverFilter = $this->buildQueryStringFromData($ss_filters, $params);
            if (!empty($serverFilter)) {
                if (empty($filter)) {
                    $filterArray = $serverFilter;
                } else {
                    $filterArray = ['AND', $filterArray, $serverFilter];
                }
            }

            return ['where' => $filterArray, 'params' => $params];
        }
    }

    /**
     * @param    string       $filter
     * @param array           $params
     * @param  ColumnSchema[] $fields_info
     *
     * @return string
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseFilterString($filter, array &$params, $fields_info)
    {
        if (empty($filter)) {
            return null;
        }

        $search = [' or ', ' and ', ' nor '];
        $replace = [' OR ', ' AND ', ' NOR '];
        $filter = trim(str_ireplace($search, $replace, $filter));

        // handle logical operators first
        $ops = array_map('trim', explode(' OR ', $filter));
        if (count($ops) > 1) {
            $parts = [];
            foreach ($ops as $op) {
                $parts[] = static::parseFilterString($op, $params, $fields_info);
            }

            return implode(' OR ', $parts);
        }

        $ops = array_map('trim', explode(' NOR ', $filter));
        if (count($ops) > 1) {
            $parts = [];
            foreach ($ops as $op) {
                $parts[] = static::parseFilterString($op, $params, $fields_info);
            }

            return implode(' NOR ', $parts);
        }

        $ops = array_map('trim', explode(' AND ', $filter));
        if (count($ops) > 1) {
            $parts = [];
            foreach ($ops as $op) {
                $parts[] = static::parseFilterString($op, $params, $fields_info);
            }

            return implode(' AND ', $parts);
        }

        if ((0 === strpos($filter, '(')) && ((strlen($filter) - 1) === strpos($filter, ')'))) {
            $filter = trim(substr($filter, 1, strlen($filter) - 2));
        }

        // the rest should be comparison operators
        $search = [' eq ', ' ne ', ' gte ', ' lte ', ' gt ', ' lt ', ' in ', ' all ', ' like ', ' <> '];
        $replace = ['=', '!=', '>=', '<=', '>', '<', ' IN ', ' ALL ', ' LIKE ', '!='];
        $filter = trim(str_ireplace($search, $replace, $filter));

        // Note: order matters here!
        $sqlOperators = ['!=', '>=', '<=', '=', '>', '<', ' IN ', ' ALL ', ' LIKE '];
        foreach ($sqlOperators as $sqlOp) {
            $ops = explode($sqlOp, $filter);
            switch (count($ops)) {
                case 2:
                    $field = trim($ops[0]);
                    $negate = false;
                    if (false !== strpos($field, ' ')) {
                        $parts = explode(' ', $field);
                        if ((count($parts) > 2) || (0 !== strcasecmp($parts[1], 'not'))) {
                            // invalid field side of operator
                            throw new BadRequestException('Invalid or unparsable field in filter request.');
                        }
                        $field = $parts[0];
                        $negate = true;
                    }
                    /** @type ColumnSchema $info */
                    if (null === $info = ArrayUtils::get($fields_info, strtolower($field))) {
                        // This could be SQL injection attempt or bad field
                        throw new BadRequestException('Invalid or unparsable field in filter request.');
                    }

                    $value = trim($ops[1]);
                    switch ($sqlOp) {
                        case ' IN ':
                        case ' ALL ':
                            $value = trim($value, '()[]');
                            $parsed = [];
                            foreach (explode(',', $value) as $each) {
                                $parsed[] = $this->parseFilterValue($each, $info, $params);
                            }
                            $value = '(' . implode(',', $parsed) . ')';
                            break;
                        default:
                            $value = $this->parseFilterValue($value, $info, $params);
                            break;
                    }

                    if ($negate) {
                        $sqlOp = 'NOT ' . $sqlOp;
                    }

                    return "{$info->rawName} $sqlOp $value";
            }
        }

        if (' IS NULL' === substr($filter, -8)) {
            $field = trim(substr($filter, 0, -8));
            /** @type ColumnSchema $info */
            if (null === $info = ArrayUtils::get($fields_info, strtolower($field))) {
                // This could be SQL injection attempt or bad field
                throw new BadRequestException('Invalid or unparsable field in filter request.');
            }

            return $info->rawName . ' IS NULL';
        }

        if (' IS NOT NULL' === substr($filter, -12)) {
            $field = trim(substr($filter, 0, -12));
            /** @type ColumnSchema $info */
            if (null === $info = ArrayUtils::get($fields_info, strtolower($field))) {
                // This could be SQL injection attempt or bad field
                throw new BadRequestException('Invalid or unparsable field in filter request.');
            }

            return $info->rawName . ' IS NOT NULL';
        }

        // This could be SQL injection attempt or unsupported filter arrangement
        throw new BadRequestException('Invalid or unparsable filter request.');
    }

    /**
     * @param mixed        $value
     * @param ColumnSchema $info
     * @param array        $params
     *
     * @return int|null|string
     * @throws BadRequestException
     */
    protected function parseFilterValue($value, ColumnSchema $info, array &$params)
    {
        if (0 !== strpos($value, ':')) {
            // if not already a replacement parameter, evaluate it
            $value = $this->dbConn->getSchema()->parseValueForSet($value, $info);

            switch ($cnvType = DbUtilities::determinePhpConversionType($info->type)) {
                case 'int':
                    if (!is_int($value)) {
                        if (!(ctype_digit($value))) {
                            throw new BadRequestException("Field '{$info->getName(true)}' must be a valid integer.");
                        } else {
                            $value = intval($value);
                        }
                    }
                    break;

                case 'time':
                    $cfgFormat = Config::get('df.db_time_format');
                    $outFormat = 'H:i:s.u';
                    $value = DbUtilities::formatDateTime($outFormat, $value, $cfgFormat);
                    break;
                case 'date':
                    $cfgFormat = Config::get('df.db_date_format');
                    $outFormat = 'Y-m-d';
                    $value = DbUtilities::formatDateTime($outFormat, $value, $cfgFormat);
                    break;
                case 'datetime':
                    $cfgFormat = Config::get('df.db_datetime_format');
                    $outFormat = 'Y-m-d H:i:s';
                    $value = DbUtilities::formatDateTime($outFormat, $value, $cfgFormat);
                    break;
                case 'timestamp':
                    $cfgFormat = Config::get('df.db_timestamp_format');
                    $outFormat = 'Y-m-d H:i:s';
                    $value = DbUtilities::formatDateTime($outFormat, $value, $cfgFormat);
                    break;

                default:
                    break;
            }

            $paramName = ':cf_' . count($params); // positionally unique
            $params[$paramName] = $value;
            $value = $paramName;
        }

        return $value;
    }

    /**
     * @throws \Exception
     */
    protected function getCurrentTimestamp()
    {
        $this->dbConn->getSchema()->getTimestampForSet();
    }

    /**
     * @param mixed        $value
     * @param ColumnSchema $field_info
     *
     * @return mixed
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseValueForSet($value, $field_info)
    {
        if (!is_null($value) && !($value instanceof Expression)) {
            $value = $this->dbConn->getSchema()->parseValueForSet($value, $field_info);

            switch ($cnvType = DbUtilities::determinePhpConversionType($field_info->type)) {
                case 'int':
                    if (!is_int($value)) {
                        if (('' === $value) && $field_info->allowNull) {
                            $value = null;
                        } elseif (!ctype_digit($value)) {
                            if (!is_float($value)) { // bigint catch as float
                                throw new BadRequestException("Field '{$field_info->getName(true)}' must be a valid integer.");
                            }
                        } else {
                            $value = intval($value);
                        }
                    }
                    break;

                case 'time':
                    $cfgFormat = Config::get('df.db_time_format');
                    $outFormat = 'H:i:s.u';
                    $value = DbUtilities::formatDateTime($outFormat, $value, $cfgFormat);
                    break;
                case 'date':
                    $cfgFormat = Config::get('df.db_date_format');
                    $outFormat = 'Y-m-d';
                    $value = DbUtilities::formatDateTime($outFormat, $value, $cfgFormat);
                    break;
                case 'datetime':
                    $cfgFormat = Config::get('df.db_datetime_format');
                    $outFormat = 'Y-m-d H:i:s';
                    $value = DbUtilities::formatDateTime($outFormat, $value, $cfgFormat);
                    break;
                case 'timestamp':
                    $cfgFormat = Config::get('df.db_timestamp_format');
                    $outFormat = 'Y-m-d H:i:s';
                    $value = DbUtilities::formatDateTime($outFormat, $value, $cfgFormat);
                    break;

                default:
                    break;
            }
        }

        return $value;
    }

    /**
     * @param array $record
     *
     * @return array
     */
    public static function interpretRecordValues($record)
    {
        if (!is_array($record) || empty($record)) {
            return $record;
        }

        foreach ($record as $field => $value) {
            Session::replaceLookups($value);
            static::valueToExpression($value);
            $record[$field] = $value;
        }

        return $record;
    }

    /**
     * @param $value
     */
    public static function valueToExpression(&$value)
    {
        if (is_array($value) && isset($value['expression'])) {
            $expression = $value['expression'];
            $params = [];
            if (is_array($expression) && isset($expression['value'])) {
                $params = isset($expression['params']) ? $expression['params'] : [];
                $expression = $expression['value'];
            }

            $value = new Expression($expression, $params);
        }
    }

    /**
     * @param string $table
     * @param array  $record Record containing relationships by name if any
     * @param array  $id     Array of id field and value, only one supported currently
     * @param array  $avail_relations
     * @param bool   $allow_delete
     *
     * @throws InternalServerErrorException
     * @return void
     */
    protected function updateRelations($table, $record, $id, $avail_relations, $allow_delete = false)
    {
        // update currently only supports one id field
        if (is_array($id)) {
            reset($id);
            $id = @current($id);
        }

        $keys = array_keys($record);
        $values = array_values($record);
        foreach ($avail_relations as $relationInfo) {
            $name = ArrayUtils::get($relationInfo, 'name');
            $pos = array_search($name, $keys);
            if (false !== $pos) {
                $relations = ArrayUtils::get($values, $pos);
                $relationType = ArrayUtils::get($relationInfo, 'type');
                switch ($relationType) {
                    case 'belongs_to':
                        /*
                    "name": "role_by_role_id",
                    "type": "belongs_to",
                    "ref_table": "role",
                    "ref_field": "id",
                    "field": "role_id"
                    */
                        // todo handle this?
                        break;
                    case 'has_many':
                        /*
                    "name": "users_by_last_modified_by_id",
                    "type": "has_many",
                    "ref_table": "user",
                    "ref_field": "last_modified_by_id",
                    "field": "id"
                    */
                        $relatedTable = ArrayUtils::get($relationInfo, 'ref_table');
                        $relatedField = ArrayUtils::get($relationInfo, 'ref_fields');
                        $this->assignManyToOne(
                            $table,
                            $id,
                            $relatedTable,
                            $relatedField,
                            $relations,
                            $allow_delete
                        );
                        break;
                    case 'many_many':
                        /*
                    "name": "roles_by_user",
                    "type": "many_many",
                    "ref_table": "role",
                    "ref_field": "id",
                    "join": "user(default_app_id,role_id)"
                    */
                        $relatedTable = ArrayUtils::get($relationInfo, 'ref_table');
                        $join = ArrayUtils::get($relationInfo, 'join', '');
                        $joinTable = substr($join, 0, strpos($join, '('));
                        $other = explode(',', substr($join, strpos($join, '(') + 1, -1));
                        $joinLeftField = trim(ArrayUtils::get($other, 0, ''));
                        $joinRightField = trim(ArrayUtils::get($other, 1, ''));
                        $this->assignManyToOneByMap(
                            $table,
                            $id,
                            $relatedTable,
                            $joinTable,
                            $joinLeftField,
                            $joinRightField,
                            $relations
                        );
                        break;
                    default:
                        throw new InternalServerErrorException('Invalid relationship type detected.');
                        break;
                }
                unset($keys[$pos]);
                unset($values[$pos]);
            }
        }
    }

    /**
     * @param array $record
     *
     * @return string
     */
    protected function parseRecordForSqlInsert($record)
    {
        $values = '';
        foreach ($record as $value) {
            $fieldVal = (is_null($value)) ? "NULL" : $this->dbConn->quoteValue($value);
            $values .= (!empty($values)) ? ',' : '';
            $values .= $fieldVal;
        }

        return $values;
    }

    /**
     * @param array $record
     *
     * @return string
     */
    protected function parseRecordForSqlUpdate($record)
    {
        $out = '';
        foreach ($record as $key => $value) {
            $fieldVal = (is_null($value)) ? "NULL" : $this->dbConn->quoteValue($value);
            $out .= (!empty($out)) ? ',' : '';
            $out .= "$key = $fieldVal";
        }

        return $out;
    }

    /**
     * @param  string|array   $fields
     * @param  ColumnSchema[] $avail_fields
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseFieldsForSqlSelect($fields, $avail_fields)
    {
        $outArray = [];
        $bindArray = [];
        if (!(empty($fields) || (ApiOptions::FIELDS_ALL === $fields))) {
            $fields = (!is_array($fields)) ? array_map('trim', explode(',', trim($fields, ','))) : $fields;
            foreach ($fields as $field) {
                $ndx = strtolower($field);
                if (!isset($avail_fields[$ndx])) {
                    throw new BadRequestException('Invalid field requested: ' . $field);
                }

                $fieldInfo = $avail_fields[$ndx];
                $bindArray[] = $this->dbConn->getSchema()->parseFieldForBinding($fieldInfo);
                $outArray[] = $this->dbConn->getSchema()->parseFieldForSelect($fieldInfo);
            }
        } else {
            foreach ($avail_fields as $fieldInfo) {
                $bindArray[] = $this->dbConn->getSchema()->parseFieldForBinding($fieldInfo);
                $outArray[] = $this->dbConn->getSchema()->parseFieldForSelect($fieldInfo);
            }
        }

        return ['fields' => $outArray, 'bindings' => $bindArray];
    }

    /**
     * @param string|array    $fields
     * @param  ColumnSchema[] $avail_fields
     * @param string          $prefix
     *
     * @throws BadRequestException
     * @return string
     */
    public function parseOutFields($fields, $avail_fields, $prefix = 'INSERTED')
    {
        if (empty($fields)) {
            return '';
        }

        $out_str = '';
        $field_arr = array_map('trim', explode(',', $fields));
        foreach ($field_arr as $field) {
            if (null === $info = ArrayUtils::get($avail_fields, $field)) {
                throw new BadRequestException("Invalid field '$field' selected for output.");
            }
            if (!empty($out_str)) {
                $out_str .= ', ';
            }
            $out_str .= $prefix . '.' . $info->rawName;
        }

        return $out_str;
    }

    // generic assignments

    /**
     * @param $relations
     * @param $data
     * @param $requests
     *
     * @throws InternalServerErrorException
     * @throws BadRequestException
     * @return array
     */
    protected function retrieveRelatedRecords($data, $relations, $requests)
    {
        if (empty($relations) || empty($requests)) {
            return $data;
        }

        $relatedData = [];
        $relatedExtras = [ApiOptions::LIMIT => static::MAX_RECORDS_RETURNED, ApiOptions::FIELDS => '*'];
        if ('*' == $requests) {
            foreach ($relations as $name => $relation) {
                if (empty($relation)) {
                    throw new BadRequestException("Empty relationship '$name' found.");
                }
                $relatedData[$name] = $this->retrieveRelationRecords($data, $relation, $relatedExtras);
            }
        } else {
            foreach ($requests as $request) {
                $name = ArrayUtils::get($request, 'name');
                $relation = ArrayUtils::get($relations, $name);
                if (empty($relation)) {
                    throw new BadRequestException("Invalid relationship '$name' requested.");
                }

                $relatedExtras['fields'] = ArrayUtils::get($request, 'fields');
                $relatedData[$name] = $this->retrieveRelationRecords($data, $relation, $relatedExtras);
            }
        }

        return array_merge($data, $relatedData);
    }

    /**
     * @param $data
     * @param $relation
     * @param $extras
     *
     * @return array|null
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     * @throws \DreamFactory\Core\Exceptions\NotFoundException
     * @throws \DreamFactory\Core\Exceptions\RestException
     * @throws \Exception
     */
    protected function retrieveRelationRecords($data, $relation, $extras)
    {
        if (empty($relation)) {
            return null;
        }

        $relationType = ArrayUtils::get($relation, 'type');
        $relatedTable = ArrayUtils::get($relation, 'ref_table');
        $relatedField = ArrayUtils::get($relation, 'ref_fields');
        $field = ArrayUtils::get($relation, 'field');
        $fieldVal = ArrayUtils::get($data, $field);

        // do we have permission to do so?
        $this->validateTableAccess($relatedTable, Verbs::GET);

        switch ($relationType) {
            case 'belongs_to':
                if (empty($fieldVal)) {
                    return null;
                }

                $fields = ArrayUtils::get($extras, ApiOptions::FIELDS);
                $ssFilters = ArrayUtils::get($extras, 'ss_filters');
                $fieldsInfo = $this->getFieldsInfo($relatedTable);
                $result = $this->parseFieldsForSqlSelect($fields, $fieldsInfo);
                $bindings = ArrayUtils::get($result, 'bindings');
                $fields = ArrayUtils::get($result, 'fields');
                $fields = (empty($fields)) ? '*' : $fields;

                // build filter string if necessary, add server-side filters if necessary
                $criteria =
                    $this->convertFilterToNative("$relatedField = $fieldVal", [], $ssFilters, $fieldsInfo);
                $where = ArrayUtils::get($criteria, 'where');
                $params = ArrayUtils::get($criteria, 'params', []);

                $relatedRecords = $this->recordQuery($relatedTable, $fields, $where, $params, $bindings, $extras);
                if (!empty($relatedRecords)) {
                    return ArrayUtils::get($relatedRecords, 0);
                }
                break;
            case 'has_many':
                if (empty($fieldVal)) {
                    return [];
                }

                $fields = ArrayUtils::get($extras, ApiOptions::FIELDS);
                $ssFilters = ArrayUtils::get($extras, 'ss_filters');
                $fieldsInfo = $this->getFieldsInfo($relatedTable);
                $result = $this->parseFieldsForSqlSelect($fields, $fieldsInfo);
                $bindings = ArrayUtils::get($result, 'bindings');
                $fields = ArrayUtils::get($result, 'fields');
                $fields = (empty($fields)) ? '*' : $fields;

                // build filter string if necessary, add server-side filters if necessary
                $criteria =
                    $this->convertFilterToNative("$relatedField = $fieldVal", [], $ssFilters, $fieldsInfo);
                $where = ArrayUtils::get($criteria, 'where');
                $params = ArrayUtils::get($criteria, 'params', []);

                return $this->recordQuery($relatedTable, $fields, $where, $params, $bindings, $extras);
                break;
            case 'many_many':
                if (empty($fieldVal)) {
                    return [];
                }

                $join = ArrayUtils::get($relation, 'join', '');
                $joinTable = substr($join, 0, strpos($join, '('));
                $other = explode(',', substr($join, strpos($join, '(') + 1, -1));
                $joinLeftField = trim(ArrayUtils::get($other, 0));
                $joinRightField = trim(ArrayUtils::get($other, 1));
                if (!empty($joinLeftField) && !empty($joinRightField)) {
                    $fieldsInfo = $this->getFieldsInfo($joinTable);
                    $result = $this->parseFieldsForSqlSelect($joinRightField, $fieldsInfo);
                    $bindings = ArrayUtils::get($result, 'bindings');
                    $fields = ArrayUtils::get($result, 'fields');
                    $fields = (empty($fields)) ? '*' : $fields;

                    // build filter string if necessary, add server-side filters if necessary
                    $junctionFilter = "( $joinRightField IS NOT NULL ) AND ( $joinLeftField = $fieldVal )";
                    $criteria = $this->convertFilterToNative($junctionFilter, [], [], $fieldsInfo);
                    $where = ArrayUtils::get($criteria, 'where');
                    $params = ArrayUtils::get($criteria, 'params', []);

                    $joinData = $this->recordQuery($joinTable, $fields, $where, $params, $bindings, $extras);
                    if (empty($joinData)) {
                        return [];
                    }

                    $relatedIds = [];
                    foreach ($joinData as $record) {
                        if (null !== $rightValue = ArrayUtils::get($record, $joinRightField)) {
                            $relatedIds[] = $rightValue;
                        }
                    }
                    if (!empty($relatedIds)) {
                        $fields = ArrayUtils::get($extras, ApiOptions::FIELDS);
                        $fieldsInfo = $this->getFieldsInfo($relatedTable);
                        $result = $this->parseFieldsForSqlSelect($fields, $fieldsInfo);
                        $bindings = ArrayUtils::get($result, 'bindings');
                        $fields = ArrayUtils::get($result, 'fields');
                        $fields = (empty($fields)) ? '*' : $fields;

                        $where = ['in', $relatedField, $relatedIds];
                        $params = [];

                        return $this->recordQuery(
                            $relatedTable,
                            $fields,
                            $where,
                            $params,
                            $bindings,
                            $extras
                        );
                    }
                }
                break;
            default:
                throw new InternalServerErrorException('Invalid relationship type detected.');
                break;
        }

        return null;
    }

    /**
     * @param string $one_table
     * @param string $one_id
     * @param string $many_table
     * @param string $many_field
     * @param array  $many_records
     * @param bool   $allow_delete
     *
     * @throws BadRequestException
     * @return void
     */
    protected function assignManyToOne(
        $one_table,
        $one_id,
        $many_table,
        $many_field,
        $many_records = [],
        $allow_delete = false
    ){
        if (empty($one_id)) {
            throw new BadRequestException("The $one_table id can not be empty.");
        }

        try {
            $manyFields = $this->getFieldsInfo($many_table);
            $pksInfo = DbUtilities::getPrimaryKeys($manyFields);
            /** @type ColumnSchema $fieldInfo */
            if (null === $fieldInfo = ArrayUtils::get($manyFields, strtolower($many_field))) {
                throw new InternalServerErrorException("Relationship field '$many_field' not found in schema.");
            }

            $deleteRelated = (!$fieldInfo->allowNull && $allow_delete);
            $relateMany = [];
            $disownMany = [];
            $insertMany = [];
            $updateMany = [];
            $upsertMany = [];
            $deleteMany = [];

            foreach ($many_records as $item) {
                if (1 === count($pksInfo)) {
                    $pkAutoSet = $pksInfo[0]->autoIncrement;
                    $pkField = $pksInfo[0]->name;
                    $id = ArrayUtils::get($item, $pkField);
                    if (empty($id)) {
                        if (!$pkAutoSet) {
                            throw new BadRequestException("Related record has no primary key value for '$pkField'.");
                        }

                        // create new child record
                        $item[$many_field] = $one_id; // assign relationship
                        $insertMany[] = $item;
                    } else {
                        if (array_key_exists($many_field, $item)) {
                            if (null == ArrayUtils::get($item, $many_field, null, true)) {
                                // disown this child or delete them
                                if ($deleteRelated) {
                                    $deleteMany[] = $id;
                                } elseif (count($item) > 1) {
                                    $item[$many_field] = null; // assign relationship
                                    $updateMany[] = $item;
                                } else {
                                    $disownMany[] = $id;
                                }

                                continue;
                            }
                        }

                        // update or upsert this child
                        if (count($item) > 1) {
                            $item[$many_field] = $one_id; // assign relationship
                            if ($pkAutoSet) {
                                $updateMany[] = $item;
                            } else {
                                $upsertMany[$id] = $item;
                            }
                        } else {
                            $relateMany[] = $id;
                        }
                    }
                } else {
                    // todo How to handle multiple primary keys?
                    throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                }
            }

            /** @var Command $command */
            $command = $this->dbConn->createCommand();

            // resolve any upsert situations
            if (!empty($upsertMany)) {
                $checkIds = array_keys($upsertMany);
                // disown/un-relate/unlink linked children
                $where = [];
                $params = [];
                if (1 === count($pksInfo)) {
                    $pkField = $pksInfo[0]->name;
                    $where[] = ['in', $pkField, $checkIds];
                } else {
                    // todo How to handle multiple primary keys?
                    throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                }

                if (count($where) > 1) {
                    array_unshift($where, 'AND');
                } else {
                    $where = $where[0];
                }

                $result = $this->parseFieldsForSqlSelect($pkField, $manyFields);
                $bindings = ArrayUtils::get($result, 'bindings');
                $fields = ArrayUtils::get($result, 'fields');
                $fields = (empty($fields)) ? '*' : $fields;
                $matchIds = $this->recordQuery($many_table, $fields, $where, $params, $bindings, null);
                unset($matchIds['meta']);

                foreach ($upsertMany as $uId => $record) {
                    if ($found = DbUtilities::findRecordByNameValue($matchIds, $pkField, $uId)) {
                        $updateMany[] = $record;
                    } else {
                        $insertMany[] = $record;
                    }
                }
            }

            if (!empty($insertMany)) {
                // create new children
                // do we have permission to do so?
                $this->validateTableAccess($many_table, Verbs::POST);
                $ssFilters = []; // TODO Session::getServiceFilters( Verbs::POST, $this->apiName, $many_table );
                foreach ($insertMany as $record) {
                    $parsed = $this->parseRecord($record, $manyFields, $ssFilters);
                    if (empty($parsed)) {
                        throw new BadRequestException('No valid fields were found in record.');
                    }

                    $rows = $command->insert($many_table, $parsed);
                    if (0 >= $rows) {
                        throw new InternalServerErrorException("Creating related $many_table records failed.");
                    }
                }
            }

            if (!empty($deleteMany)) {
                // destroy linked children that can't stand alone - sounds sinister
                // do we have permission to do so?
                $this->validateTableAccess($many_table, Verbs::DELETE);
                $ssFilters = []; // TODO Session::getServiceFilters( Verbs::DELETE, $this->name, $many_table );
                $where = [];
                $params = [];
                if (1 === count($pksInfo)) {
                    $pkField = $pksInfo[0]->name;
                    $where[] = ['in', $pkField, $deleteMany];
                } else {
                    // todo How to handle multiple primary keys?
                    throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                }
                $serverFilter = $this->buildQueryStringFromData($ssFilters, $params);
                if (!empty($serverFilter)) {
                    $where[] = $serverFilter;
                }

                if (count($where) > 1) {
                    array_unshift($where, 'AND');
                } else {
                    $where = $where[0];
                }

                $command->delete($many_table, $where, $params);
//                if ( 0 >= $rows )
//                {
//                    throw new NotFoundException( "Deleting related $many_table records failed." );
//                }
            }

            if (!empty($updateMany) || !empty($relateMany) || !empty($disownMany)) {
                // do we have permission to do so?
                $this->validateTableAccess($many_table, Verbs::PUT);
                $ssFilters = []; // TODO Session::getServiceFilters( Verbs::PUT, $this->apiName, $many_table );

                if (!empty($updateMany)) {
                    // update existing and adopt new children
                    $where = [];
                    $params = [];
                    if (1 === count($pksInfo)) {
                        $pkField = $pksInfo[0]->name;
                        $where[] = $this->dbConn->quoteColumnName($pkField) . " = :pk_$pkField";
                    } else {
                        // todo How to handle multiple primary keys?
                        throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                    }
                    $serverFilter = $this->buildQueryStringFromData($ssFilters, $params);
                    if (!empty($serverFilter)) {
                        $where[] = $serverFilter;
                    }

                    if (count($where) > 1) {
                        array_unshift($where, 'AND');
                    } else {
                        $where = $where[0];
                    }

                    foreach ($updateMany as $record) {
                        if (1 === count($pksInfo)) {
                            $pkField = $pksInfo[0]->name;
                            $params[":pk_$pkField"] = ArrayUtils::get($record, $pkField);
                        } else {
                            // todo How to handle multiple primary keys?
                            throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                        }
                        $parsed = $this->parseRecord($record, $manyFields, $ssFilters, true);
                        if (empty($parsed)) {
                            throw new BadRequestException('No valid fields were found in record.');
                        }

                        $command->update($many_table, $parsed, $where, $params);
//                        if ( 0 >= $rows )
//                        {
//                            throw new InternalServerErrorException( "Updating related $many_table records failed." );
//                        }
                    }
                }

                if (!empty($relateMany)) {
                    // adopt/relate/link unlinked children
                    $where = [];
                    $params = [];
                    if (1 === count($pksInfo)) {
                        $pkField = $pksInfo[0]->name;
                        $where[] = ['in', $pkField, $relateMany];
                    } else {
                        // todo How to handle multiple primary keys?
                        throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                    }
                    $serverFilter = $this->buildQueryStringFromData($ssFilters, $params);
                    if (!empty($serverFilter)) {
                        $where[] = $serverFilter;
                    }

                    if (count($where) > 1) {
                        array_unshift($where, 'AND');
                    } else {
                        $where = $where[0];
                    }

                    $updates = [$many_field => $one_id];
                    $parsed = $this->parseRecord($updates, $manyFields, $ssFilters, true);
                    if (!empty($parsed)) {
                        $command->update($many_table, $parsed, $where, $params);
//                        if ( 0 >= $rows )
//                        {
//                            throw new InternalServerErrorException( "Updating related $many_table records failed." );
//                        }
                    }
                }

                if (!empty($disownMany)) {
                    // disown/un-relate/unlink linked children
                    $where = [];
                    $params = [];
                    if (1 === count($pksInfo)) {
                        $pkField = $pksInfo[0]->name;
                        $where[] = ['in', $pkField, $disownMany];
                    } else {
                        // todo How to handle multiple primary keys?
                        throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                    }
                    $serverFilter = $this->buildQueryStringFromData($ssFilters, $params);
                    if (!empty($serverFilter)) {
                        $where[] = $serverFilter;
                    }

                    if (count($where) > 1) {
                        array_unshift($where, 'AND');
                    } else {
                        $where = $where[0];
                    }

                    $updates = [$many_field => null];
                    $parsed = $this->parseRecord($updates, $manyFields, $ssFilters, true);
                    if (!empty($parsed)) {
                        $command->update($many_table, $parsed, $where, $params);
//                        if ( 0 >= $rows )
//                        {
//                            throw new NotFoundException( 'No records were found using the given identifiers.' );
//                        }
                    }
                }
            }
        } catch (\Exception $ex) {
            throw new BadRequestException("Failed to update many to one assignment.\n{$ex->getMessage()}");
        }
    }

    /**
     * @param string $one_table
     * @param mixed  $one_id
     * @param string $many_table
     * @param string $map_table
     * @param string $one_field
     * @param string $many_field
     * @param array  $many_records
     *
     * @throws InternalServerErrorException
     * @throws BadRequestException
     * @return void
     */
    protected function assignManyToOneByMap(
        $one_table,
        $one_id,
        $many_table,
        $map_table,
        $one_field,
        $many_field,
        $many_records = []
    ){
        if (empty($one_id)) {
            throw new BadRequestException("The $one_table id can not be empty.");
        }

        try {
            $oneFields = $this->getFieldsInfo($one_table);
            $pkOneField = DbUtilities::getPrimaryKeyFieldFromDescribe($oneFields);
            $manyFields = $this->getFieldsInfo($many_table);
            $pksManyInfo = DbUtilities::getPrimaryKeys($manyFields);
            $mapFields = $this->getFieldsInfo($map_table);
//			$pkMapField = static::getPrimaryKeyFieldFromDescribe( $mapFields );

            $result = $this->parseFieldsForSqlSelect($many_field, $mapFields);
            $bindings = ArrayUtils::get($result, 'bindings');
            $fields = ArrayUtils::get($result, 'fields');
            $fields = (empty($fields)) ? '*' : $fields;
            $params[":f_$one_field"] = $one_id;
            $where = $this->dbConn->quoteColumnName($one_field) . " = :f_$one_field";
            $maps = $this->recordQuery($map_table, $fields, $where, $params, $bindings, null);
            unset($maps['meta']);

            $createMap = []; // map records to create
            $deleteMap = []; // ids of 'many' records to delete from maps
            $insertMany = [];
            $updateMany = [];
            $upsertMany = [];
            foreach ($many_records as $item) {
                if (1 === count($pksManyInfo)) {
                    $pkAutoSet = $pksManyInfo[0]->autoIncrement;
                    $pkManyField = $pksManyInfo[0]->name;
                    $id = ArrayUtils::get($item, $pkManyField);
                    if (empty($id)) {
                        if (!$pkAutoSet) {
                            throw new BadRequestException("Related record has no primary key value for '$pkManyField'.");
                        }

                        // create new child record
                        $insertMany[] = $item;
                    } else {
                        // pk fields exists, must be dealing with existing 'many' record
                        $oneLookup = "$one_table.$pkOneField";
                        if (array_key_exists($oneLookup, $item)) {
                            if (null == ArrayUtils::get($item, $oneLookup, null, true)) {
                                // delete this relationship
                                $deleteMap[] = $id;
                                continue;
                            }
                        }

                        // update the 'many' record if more than the above fields
                        if (count($item) > 1) {
                            if ($pkAutoSet) {
                                $updateMany[] = $item;
                            } else {
                                $upsertMany[$id] = $item;
                            }
                        }

                        // if relationship doesn't exist, create it
                        foreach ($maps as $map) {
                            if (ArrayUtils::get($map, $many_field) == $id) {
                                continue 2; // got what we need from this one
                            }
                        }

                        $createMap[] = [$many_field => $id, $one_field => $one_id];
                    }
                } else {
                    // todo How to handle multiple primary keys?
                    throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                }
            }

            /** @var Command $command */
            $command = $this->dbConn->createCommand();

            // resolve any upsert situations
            if (!empty($upsertMany)) {
                $checkIds = array_keys($upsertMany);
                // disown/un-relate/unlink linked children
                $where = [];
                $params = [];
                if (1 === count($pksManyInfo)) {
                    $pkField = $pksManyInfo[0]->name;
                    $where[] = ['in', $pkField, $checkIds];
                } else {
                    // todo How to handle multiple primary keys?
                    throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                }

                if (count($where) > 1) {
                    array_unshift($where, 'AND');
                } else {
                    $where = $where[0];
                }

                $result = $this->parseFieldsForSqlSelect($pkField, $manyFields);
                $bindings = ArrayUtils::get($result, 'bindings');
                $fields = ArrayUtils::get($result, 'fields');
                $fields = (empty($fields)) ? '*' : $fields;
                $matchIds = $this->recordQuery($many_table, $fields, $where, $params, $bindings, null);
                unset($matchIds['meta']);

                foreach ($upsertMany as $uId => $record) {
                    if ($found = DbUtilities::findRecordByNameValue($matchIds, $pkField, $uId)) {
                        $updateMany[] = $record;
                    } else {
                        $insertMany[] = $record;
                    }
                }
            }

            if (!empty($insertMany)) {
                // do we have permission to do so?
                $this->validateTableAccess($many_table, Verbs::POST);
                $ssManyFilters = []; // TDOO Session::getServiceFilters( Verbs::POST, $this->apiName, $many_table );
                // create new many records
                foreach ($insertMany as $record) {
                    $parsed = $this->parseRecord($record, $manyFields, $ssManyFilters);
                    if (empty($parsed)) {
                        throw new BadRequestException('No valid fields were found in record.');
                    }

                    $rows = $command->insert($many_table, $parsed);
                    if (0 >= $rows) {
                        throw new InternalServerErrorException("Creating related $many_table records failed.");
                    }

                    $manyId = (int)$this->dbConn->lastInsertID;
                    if (!empty($manyId)) {
                        $createMap[] = [$many_field => $manyId, $one_field => $one_id];
                    }
                }
            }

            if (!empty($updateMany)) {
                // update existing many records
                // do we have permission to do so?
                $this->validateTableAccess($many_table, Verbs::PUT);
                $ssManyFilters = []; // TODO Session::getServiceFilters( Verbs::PUT, $this->apiName, $many_table );

                $where = [];
                $params = [];
                if (1 === count($pksManyInfo)) {
                    $pkField = $pksManyInfo[0]->name;
                    $where[] = $this->dbConn->quoteColumnName($pkField) . " = :pk_$pkField";
                } else {
                    // todo How to handle multiple primary keys?
                    throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                }

                $serverFilter = $this->buildQueryStringFromData($ssManyFilters, $params);
                if (!empty($serverFilter)) {
                    $where[] = $serverFilter;
                }

                if (count($where) > 1) {
                    array_unshift($where, 'AND');
                } else {
                    $where = $where[0];
                }

                foreach ($updateMany as $record) {
                    $params[":pk_$pkField"] = ArrayUtils::get($record, $pkField);
                    $parsed = $this->parseRecord($record, $manyFields, $ssManyFilters, true);
                    if (empty($parsed)) {
                        throw new BadRequestException('No valid fields were found in record.');
                    }

                    $command->update($many_table, $parsed, $where, $params);
//                        if ( 0 >= $rows )
//                        {
//                            throw new InternalServerErrorException( "Updating related $many_table records failed." );
//                        }
                }
            }

            if (!empty($createMap)) {
                // do we have permission to do so?
                $this->validateTableAccess($map_table, Verbs::POST);
                $ssMapFilters = []; // TODO Session::getServiceFilters( Verbs::POST, $this->apiName, $map_table );
                foreach ($createMap as $record) {
                    $parsed = $this->parseRecord($record, $mapFields, $ssMapFilters);
                    if (empty($parsed)) {
                        throw new BadRequestException("No valid fields were found in related $map_table record.");
                    }

                    $rows = $command->insert($map_table, $parsed);
                    if (0 >= $rows) {
                        throw new InternalServerErrorException("Creating related $map_table records failed.");
                    }
                }
            }

            if (!empty($deleteMap)) {
                // do we have permission to do so?
                $this->validateTableAccess($map_table, Verbs::DELETE);
                $ssMapFilters = []; // TODO Session::getServiceFilters( Verbs::DELETE, $this->apiName, $map_table );
                $where = [];
                $params = [];
                $where[] = $this->dbConn->quoteColumnName($one_field) . " = '$one_id'";
                $where[] = ['in', $many_field, $deleteMap];
                $serverFilter = $this->buildQueryStringFromData($ssMapFilters, $params);
                if (!empty($serverFilter)) {
                    $where[] = $serverFilter;
                }

                if (count($where) > 1) {
                    array_unshift($where, 'AND');
                } else {
                    $where = $where[0];
                }

                $command->delete($map_table, $where, $params);
//                if ( 0 >= $rows )
//                {
//                    throw new NotFoundException( "Deleting related $map_table records failed." );
//                }
            }
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to update many to one map assignment.\n{$ex->getMessage()}");
        }
    }

    /**
     * @param       $filter_info
     * @param array $params
     *
     * @return null|string
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    protected function buildQueryStringFromData($filter_info, array &$params)
    {
        $filter_info = ArrayUtils::clean($filter_info);
        $filters = ArrayUtils::get($filter_info, 'filters');
        if (empty($filters)) {
            return null;
        }

        $sql = '';
        $combiner = ArrayUtils::get($filter_info, 'filter_op', 'and');
        foreach ($filters as $key => $filter) {
            if (!empty($sql)) {
                $sql .= " $combiner ";
            }

            $name = ArrayUtils::get($filter, 'name');
            $op = ArrayUtils::get($filter, 'operator');
            $value = ArrayUtils::get($filter, 'value');
            $value = static::interpretFilterValue($value);

            if (empty($name) || empty($op)) {
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
            }

            switch ($op) {
                case 'is null':
                case 'is not null':
                    $sql .= $this->dbConn->quoteColumnName($name) . " $op";
                    break;
                default:
                    $paramName = ':ssf_' . $name . '_' . $key;
                    $params[$paramName] = $value;
                    $value = $paramName;
                    $sql .= $this->dbConn->quoteColumnName($name) . " $op $value";
                    break;
            }
        }

        return $sql;
    }

    /**
     * Handle raw SQL Azure requests
     */
    protected function batchSqlQuery($query, $bindings = [])
    {
        if (empty($query)) {
            throw new BadRequestException('[NOQUERY]: No query string present in request.');
        }
        try {
            $reader = $this->dbConn->createCommand($query)->query();
            $dummy = [];
            foreach ($bindings as $binding) {
                $name = ArrayUtils::get($binding, 'name');
                $type = ArrayUtils::get($binding, 'pdo_type');
                $reader->bindColumn($name, $dummy[$name], $type);
            }

            $data = [];
            $rowData = [];
            while ($row = $reader->read()) {
                $rowData[] = $row;
            }
            if (1 == count($rowData)) {
                $rowData = $rowData[0];
            }
            $data[] = $rowData;

            // Move to the next result and get results
            while ($reader->nextResult()) {
                $rowData = [];
                while ($row = $reader->read()) {
                    $rowData[] = $row;
                }
                if (1 == count($rowData)) {
                    $rowData = $rowData[0];
                }
                $data[] = $rowData;
            }

            return $data;
        } catch (\Exception $ex) {
            error_log('batchquery: ' . $ex->getMessage() . PHP_EOL . $query);

            throw $ex;
        }
    }

    /**
     * Handle SQL Db requests with output as array
     */
    public function singleSqlQuery($query, $params = null)
    {
        if (empty($query)) {
            throw new BadRequestException('[NOQUERY]: No query string present in request.');
        }
        try {
            if (isset($params) && !empty($params)) {
                $data = $this->dbConn->createCommand($query)->queryAll(true, $params);
            } else {
                $data = $this->dbConn->createCommand($query)->queryAll();
            }

            return $data;
        } catch (\Exception $ex) {
            error_log('singlequery: ' . $ex->getMessage() . PHP_EOL . $query . PHP_EOL . print_r($params, true));

            throw $ex;
        }
    }

    /**
     * Handle SQL Db requests with output as array
     */
    public function singleSqlExecute($query, $params = null)
    {
        if (empty($query)) {
            throw new BadRequestException('[NOQUERY]: No query string present in request.');
        }
        try {
            if (isset($params) && !empty($params)) {
                $data = $this->dbConn->createCommand($query)->execute($params);
            } else {
                $data = $this->dbConn->createCommand($query)->execute();
            }

            return $data;
        } catch (\Exception $ex) {
            error_log('singleexecute: ' . $ex->getMessage() . PHP_EOL . $query . PHP_EOL . print_r($params, true));

            throw $ex;
        }
    }

    /**
     * @param      $table
     * @param null $fields_info
     * @param null $requested_fields
     * @param null $requested_types
     *
     * @return array|\DreamFactory\Core\Database\ColumnSchema[]
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     */
    protected function getIdsInfo($table, $fields_info = null, &$requested_fields = null, $requested_types = null)
    {
        $idsInfo = [];
        if (empty($requested_fields)) {
            $requested_fields = [];
            /** @type ColumnSchema[] $idsInfo */
            $idsInfo = DbUtilities::getPrimaryKeys($fields_info);
            foreach ($idsInfo as $info) {
                $requested_fields[] = $info->getName(true);
            }
        } else {
            if (false !== $requested_fields = DbUtilities::validateAsArray($requested_fields, ',')) {
                foreach ($requested_fields as $field) {
                    $ndx = strtolower($field);
                    if (isset($fields_info[$ndx])) {
                        $idsInfo[] = $fields_info[$ndx];
                    }
                }
            }
        }

        return $idsInfo;
    }

    /**
     * {@inheritdoc}
     */
    protected function initTransaction($table_name, &$id_fields = null, $id_types = null, $require_ids = true)
    {
        $this->transaction = null;

        return parent::initTransaction($table_name, $id_fields, $id_types, $require_ids);
    }

    /**
     * {@inheritdoc}
     */
    protected function addToTransaction(
        $record = null,
        $id = null,
        $extras = null,
        $rollback = false,
        $continue = false,
        $single = false
    ){
        if ($rollback) {
            // sql transaction really only for rollback scenario, not batching
            if (!isset($this->transaction)) {
                $this->transaction = $this->dbConn->beginTransaction();
            }
        }

        $fields = ArrayUtils::get($extras, ApiOptions::FIELDS);
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');
        $updates = ArrayUtils::get($extras, 'updates');
        $idFields = ArrayUtils::get($extras, 'id_fields');
        $needToIterate = ($single || !$continue || (1 < count($this->tableIdsInfo)));

        $related = ArrayUtils::get($extras, 'related');
        $requireMore = ArrayUtils::getBool($extras, 'require_more') || !empty($related);
        $allowRelatedDelete = ArrayUtils::getBool($extras, 'allow_related_delete', false);
        $relatedInfo = $this->describeTableRelated($this->transactionTable);

        $where = [];
        $params = [];
        if (is_array($id)) {
            foreach ($idFields as $name) {
                $where[] = $this->dbConn->quoteColumnName($name) . " = :pk_$name";
                $params[":pk_$name"] = ArrayUtils::get($id, $name);
            }
        } else {
            $name = ArrayUtils::get($idFields, 0);
            $where[] = $this->dbConn->quoteColumnName($name) . " = :pk_$name";
            $params[":pk_$name"] = $id;
        }

        $serverFilter = $this->buildQueryStringFromData($ssFilters, $params);
        if (!empty($serverFilter)) {
            $where[] = $serverFilter;
        }

        if (count($where) > 1) {
            array_unshift($where, 'AND');
        } else {
            $where = ArrayUtils::get($where, 0, null);
        }

        /** @var Command $command */
        $command = $this->dbConn->createCommand();

        $out = [];
        switch ($this->getAction()) {
            case Verbs::POST:
                $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters);
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                $rows = $command->insert($this->transactionTable, $parsed);
                if (0 >= $rows) {
                    throw new InternalServerErrorException("Record insert failed.");
                }

                if (empty($id)) {
                    $id = [];
                    foreach ($this->tableIdsInfo as $info) {
                        $idName = $info->name;
                        if ($info->autoIncrement) {
                            $schema = $this->dbConn->getSchema()->getTable($this->transactionTable);
                            $sequenceName = $schema->sequenceName;
                            $id[$idName] = (int)$this->dbConn->getLastInsertID($sequenceName);
                        } else {
                            // must have been passed in with request
                            $id[$idName] = ArrayUtils::get($parsed, $idName);
                        }
                    }
                }
                if (!empty($relatedInfo)) {
                    $this->updateRelations(
                        $this->transactionTable,
                        $record,
                        $id,
                        $relatedInfo,
                        $allowRelatedDelete
                    );
                }

                $idName =
                    (isset($this->tableIdsInfo, $this->tableIdsInfo[0], $this->tableIdsInfo[0]->name))
                        ? $this->tableIdsInfo[0]->name : null;
                $out = (is_array($id)) ? $id : [$idName => $id];

                // add via record, so batch processing can retrieve extras
                if ($requireMore) {
                    parent::addToTransaction($id);
                }
                break;

            case Verbs::PUT:
            case Verbs::MERGE:
            case Verbs::PATCH:
                if (!empty($updates)) {
                    $record = $updates;
                }

                $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters, true);

                // only update by ids can use batching, too complicated with ssFilters and related update
//                if ( !$needToIterate && !empty( $updates ) )
//                {
//                    return parent::addToTransaction( null, $id );
//                }

                if (!empty($parsed)) {
                    $rows = $command->update($this->transactionTable, $parsed, $where, $params);
                    if (0 >= $rows) {
                        // could have just not updated anything, or could be bad id
                        $fields = (empty($fields)) ? $idFields : $fields;
                        $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
                        $bindings = ArrayUtils::get($result, 'bindings');
                        $fields = ArrayUtils::get($result, 'fields');
                        $fields = (empty($fields)) ? '*' : $fields;

                        $result = $this->recordQuery(
                            $this->transactionTable,
                            $fields,
                            $where,
                            $params,
                            $bindings,
                            $extras
                        );
                        if (empty($result)) {
                            throw new NotFoundException("Record with identifier '" .
                                print_r($id, true) .
                                "' not found.");
                        }
                    }
                }

                if (!empty($relatedInfo)) {
                    $this->updateRelations(
                        $this->transactionTable,
                        $record,
                        $id,
                        $relatedInfo,
                        $allowRelatedDelete
                    );
                }

                $idName =
                    (isset($this->tableIdsInfo, $this->tableIdsInfo[0], $this->tableIdsInfo[0]->name))
                        ? $this->tableIdsInfo[0]->name : null;
                $out = (is_array($id)) ? $id : [$idName => $id];

                // add via record, so batch processing can retrieve extras
                if ($requireMore) {
                    parent::addToTransaction($id);
                }
                break;

            case Verbs::DELETE:
                if (!$needToIterate) {
                    return parent::addToTransaction(null, $id);
                }

                // add via record, so batch processing can retrieve extras
                if ($requireMore) {
                    $fields = (empty($fields)) ? $idFields : $fields;
                    $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
                    $bindings = ArrayUtils::get($result, 'bindings');
                    $fields = ArrayUtils::get($result, 'fields');
                    $fields = (empty($fields)) ? '*' : $fields;

                    $result = $this->recordQuery(
                        $this->transactionTable,
                        $fields,
                        $where,
                        $params,
                        $bindings,
                        $extras
                    );
                    if (empty($result)) {
                        throw new NotFoundException("Record with identifier '" . print_r($id, true) . "' not found.");
                    }

                    $out = $result[0];
                }

                $rows = $command->delete($this->transactionTable, $where, $params);
                if (0 >= $rows) {
                    if (empty($out)) {
                        // could have just not updated anything, or could be bad id
                        $fields = (empty($fields)) ? $idFields : $fields;
                        $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
                        $bindings = ArrayUtils::get($result, 'bindings');
                        $fields = ArrayUtils::get($result, 'fields');
                        $fields = (empty($fields)) ? '*' : $fields;

                        $result = $this->recordQuery(
                            $this->transactionTable,
                            $fields,
                            $where,
                            $params,
                            $bindings,
                            $extras
                        );
                        if (empty($result)) {
                            throw new NotFoundException("Record with identifier '" .
                                print_r($id, true) .
                                "' not found.");
                        }
                    }
                }

                if (empty($out)) {
                    $idName =
                        (isset($this->tableIdsInfo, $this->tableIdsInfo[0], $this->tableIdsInfo[0]->name))
                            ? $this->tableIdsInfo[0]->name : null;
                    $out = (is_array($id)) ? $id : [$idName => $id];
                }
                break;

            case Verbs::GET:
                if (!$needToIterate) {
                    return parent::addToTransaction(null, $id);
                }

                $fields = (empty($fields)) ? $idFields : $fields;
                $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
                $bindings = ArrayUtils::get($result, 'bindings');
                $fields = ArrayUtils::get($result, 'fields');
                $fields = (empty($fields)) ? '*' : $fields;

                $result =
                    $this->recordQuery($this->transactionTable, $fields, $where, $params, $bindings, $extras);
                if (empty($result)) {
                    throw new NotFoundException("Record with identifier '" . print_r($id, true) . "' not found.");
                }

                $out = $result[0];
                break;
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
        if (empty($this->batchRecords) && empty($this->batchIds)) {
            if (isset($this->transaction)) {
                $this->transaction->commit();
            }

            return null;
        }

        $updates = ArrayUtils::get($extras, 'updates');
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');
        $fields = ArrayUtils::get($extras, ApiOptions::FIELDS);
        $idFields = ArrayUtils::get($extras, 'id_fields');
        $related = ArrayUtils::get($extras, 'related');
        $requireMore = ArrayUtils::getBool($extras, 'require_more') || !empty($related);
        $allowRelatedDelete = ArrayUtils::getBool($extras, 'allow_related_delete', false);
        $relatedInfo = $this->describeTableRelated($this->transactionTable);

        $where = [];
        $params = [];

        $idName =
            (isset($this->tableIdsInfo, $this->tableIdsInfo[0], $this->tableIdsInfo[0]->name))
                ? $this->tableIdsInfo[0]->name : null;
        if (empty($idName)) {
            throw new BadRequestException('No valid identifier found for this table.');
        }

        if (!empty($this->batchRecords)) {
            if (is_array($this->batchRecords[0])) {
                $temp = [];
                foreach ($this->batchRecords as $record) {
                    $temp[] = ArrayUtils::get($record, $idName);
                }

                $where[] = ['in', $idName, $temp];
            } else {
                $where[] = ['in', $idName, $this->batchRecords];
            }
        } else {
            $where[] = ['in', $idName, $this->batchIds];
        }

        $serverFilter = $this->buildQueryStringFromData($ssFilters, $params);
        if (!empty($serverFilter)) {
            $where[] = $serverFilter;
        }

        if (count($where) > 1) {
            array_unshift($where, 'AND');
        } else {
            $where = $where[0];
        }

        $out = [];
        $action = $this->getAction();
        if (!empty($this->batchRecords)) {
            if (1 == count($this->tableIdsInfo)) {
                // records are used to retrieve extras
                // ids array are now more like records
                $fields = (empty($fields)) ? $idFields : $fields;
                $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
                $bindings = ArrayUtils::get($result, 'bindings');
                $fields = ArrayUtils::get($result, 'fields');
                $fields = (empty($fields)) ? '*' : $fields;

                $result =
                    $this->recordQuery($this->transactionTable, $fields, $where, $params, $bindings, $extras);
                if (empty($result)) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                $out = $result;
            } else {
                $out = $this->retrieveRecords($this->transactionTable, $this->batchRecords, $extras);
            }

            $this->batchRecords = [];
        } elseif (!empty($this->batchIds)) {
            /** @var Command $command */
            $command = $this->dbConn->createCommand();

            switch ($action) {
                case Verbs::PUT:
                case Verbs::MERGE:
                case Verbs::PATCH:
                    if (!empty($updates)) {
                        $parsed = $this->parseRecord($updates, $this->tableFieldsInfo, $ssFilters, true);
                        if (!empty($parsed)) {
                            $rows = $command->update($this->transactionTable, $parsed, $where, $params);
                            if (0 >= $rows) {
                                throw new NotFoundException('No records were found using the given identifiers.');
                            }

                            if (count($this->batchIds) !== $rows) {
                                throw new BadRequestException('Batch Error: Not all requested records could be updated.');
                            }
                        }

                        foreach ($this->batchIds as $id) {
                            if (!empty($relatedInfo)) {
                                $this->updateRelations(
                                    $this->transactionTable,
                                    $updates,
                                    $id,
                                    $relatedInfo,
                                    $allowRelatedDelete
                                );
                            }
                        }

                        if ($requireMore) {
                            $fields = (empty($fields)) ? $idFields : $fields;
                            $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
                            $bindings = ArrayUtils::get($result, 'bindings');
                            $fields = ArrayUtils::get($result, 'fields');
                            $fields = (empty($fields)) ? '*' : $fields;

                            $result = $this->recordQuery(
                                $this->transactionTable,
                                $fields,
                                $where,
                                $params,
                                $bindings,
                                $extras
                            );
                            if (empty($result)) {
                                throw new NotFoundException('No records were found using the given identifiers.');
                            }

                            $out = $result;
                        }
                    }
                    break;

                case Verbs::DELETE:
                    if ($requireMore) {
                        $fields = (empty($fields)) ? $idFields : $fields;
                        $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
                        $bindings = ArrayUtils::get($result, 'bindings');
                        $fields = ArrayUtils::get($result, 'fields');
                        $fields = (empty($fields)) ? '*' : $fields;

                        $result = $this->recordQuery(
                            $this->transactionTable,
                            $fields,
                            $where,
                            $params,
                            $bindings,
                            $extras
                        );
                        if (count($this->batchIds) !== count($result)) {
                            $errors = [];
                            foreach ($this->batchIds as $index => $id) {
                                $found = false;
                                if (empty($result)) {
                                    foreach ($result as $record) {
                                        if ($id == ArrayUtils::get($record, $idName)) {
                                            $out[$index] = $record;
                                            $found = true;
                                            continue;
                                        }
                                    }
                                }
                                if (!$found) {
                                    $errors[] = $index;
                                    $out[$index] = "Record with identifier '" . print_r($id, true) . "' not found.";
                                }
                            }
                        } else {
                            $out = $result;
                        }
                    }

                    $rows = $command->delete($this->transactionTable, $where, $params);
                    if (count($this->batchIds) !== $rows) {
                        throw new BadRequestException('Batch Error: Not all requested records were deleted.');
                    }
                    break;

                case Verbs::GET:
                    $fields = (empty($fields)) ? $idFields : $fields;
                    $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
                    $bindings = ArrayUtils::get($result, 'bindings');
                    $fields = ArrayUtils::get($result, 'fields');
                    $fields = (empty($fields)) ? '*' : $fields;

                    $result = $this->recordQuery(
                        $this->transactionTable,
                        $fields,
                        $where,
                        $params,
                        $bindings,
                        $extras
                    );
                    if (empty($result)) {
                        throw new NotFoundException('No records were found using the given identifiers.');
                    }

                    if (count($this->batchIds) !== count($result)) {
                        $errors = [];
                        foreach ($this->batchIds as $index => $id) {
                            $found = false;
                            foreach ($result as $record) {
                                if ($id == ArrayUtils::get($record, $idName)) {
                                    $out[$index] = $record;
                                    $found = true;
                                    continue;
                                }
                            }
                            if (!$found) {
                                $errors[] = $index;
                                $out[$index] = "Record with identifier '" . print_r($id, true) . "' not found.";
                            }
                        }

                        if (!empty($errors)) {
                            $context = ['error' => $errors, ResourcesWrapper::getWrapper() => $out];
                            throw new NotFoundException('Batch Error: Not all records could be retrieved.', null, null,
                                $context);
                        }
                    }

                    $out = $result;
                    break;

                default:
                    break;
            }

            if (empty($out)) {
                $out = [];
                foreach ($this->batchIds as $id) {
                    $out[] = [$idName => $id];
                }
            }

            $this->batchIds = [];
        }

        if (isset($this->transaction)) {
            $this->transaction->commit();
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        if (isset($this->transaction)) {
            $this->transaction->rollback();
        }

        return true;
    }

    /**
     * @param $type
     *
     * @return int | null
     */
    public static function determinePdoBindingType($type)
    {
        switch ($type) {
            case 'boolean':
                return \PDO::PARAM_BOOL;

            case 'integer':
            case 'id':
            case 'reference':
            case 'user_id':
            case 'user_id_on_create':
            case 'user_id_on_update':
                return \PDO::PARAM_INT;

            case 'string':
                return \PDO::PARAM_STR;
                break;
        }

        return null;
    }

    /**
     * @return array
     */
    public function getApiDocInfo()
    {
        $base = parent::getApiDocInfo();

//        $addTableParameters = [
//            [
//                'name'          => 'related',
//                'description'   => 'Comma-delimited list of relationship names to retrieve for each record, or \'*\' to retrieve all.',
//                'allowMultiple' => true,
//                'type'          => 'string',
//                'paramType'     => 'query',
//                'required'      => false,
//            ]
//        ];
//
//        $addTableNotes =
//            'Use the <b>related</b> parameter to return related records for each resource. ' .
//            'By default, no related records are returned.<br/> ';

        $base['models'] = array_merge($base['models'], static::getApiDocCommonModels());

        return $base;
    }
}