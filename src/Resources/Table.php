<?php

namespace DreamFactory\Core\SqlDb\Resources;

use Config;
use DreamFactory\Core\Components\Service2ServiceRequest;
use DreamFactory\Core\Database\ColumnSchema;
use DreamFactory\Core\Database\RelationSchema;
use DreamFactory\Core\Database\TableSchema;
use DreamFactory\Core\Database\Expression;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\ForbiddenException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\NotImplementedException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Models\Service;
use DreamFactory\Core\Resources\BaseDbTableResource;
use DreamFactory\Core\SqlDb\Components\SqlDbResource;
use DreamFactory\Core\SqlDb\Components\TableDescriber;
use DreamFactory\Core\Utility\DataFormatter;
use DreamFactory\Core\Utility\ResourcesWrapper;
use DreamFactory\Core\Utility\ServiceHandler;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Library\Utility\Enums\Verbs;
use DreamFactory\Library\Utility\Scalar;
use Illuminate\Database\Query\Builder;

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
     * @var null | bool
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
            $related = $this->request->getParameter(ApiOptions::RELATED);
            if (!empty($related) && is_string($related) && ('*' !== $related)) {
                if (!is_array($related)) {
                    $related = array_map('trim', explode(',', $related));
                }
                $relations = [];
                foreach ($related as $relative) {
                    // search for relation + '_' + option because '.' is replaced by '_'
                    $relations[strtolower($relative)] =
                        [
                            'name'             => $relative,
                            ApiOptions::FIELDS => $this->request->getParameter(
                                str_replace('.', '_', $relative . '.' . ApiOptions::FIELDS),
                                '*'),
                            ApiOptions::LIMIT  => $this->request->getParameter(
                                str_replace('.', '_', $relative . '.' . ApiOptions::LIMIT),
                                static::getMaxRecordsReturnedLimit()),
                            ApiOptions::ORDER  => $this->request->getParameter(
                                str_replace('.', '_', $relative . '.' . ApiOptions::ORDER)),
                            ApiOptions::GROUP  => $this->request->getParameter(
                                str_replace('.', '_', $relative . '.' . ApiOptions::GROUP)),
                        ];
                }

                $this->request->setParameter(ApiOptions::RELATED, $relations);
            }
        }

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function updateRecordsByFilter($table, $record, $filter = null, $params = [], $extras = [])
    {
        $record = static::validateAsArray($record, null, false, 'There are no fields in the record.');

        $idFields = array_get($extras, ApiOptions::ID_FIELD);
        $idTypes = array_get($extras, ApiOptions::ID_TYPE);
        $fields = array_get($extras, ApiOptions::FIELDS);
        $related = array_get($extras, ApiOptions::RELATED);
        $allowRelatedDelete = Scalar::boolval(array_get($extras, ApiOptions::ALLOW_RELATED_DELETE));
        $ssFilters = array_get($extras, 'ss_filters');

        try {
            $fieldsInfo = $this->getFieldsInfo($table);
            $idsInfo = $this->getIdsInfo($table, $fieldsInfo, $idFields, $idTypes);
            $relatedInfo = $this->describeTableRelated($table);
            $fields = (empty($fields)) ? $idFields : $fields;
            $result = $this->parseFieldsForSqlSelect($fields, $fieldsInfo);
            $bindings = array_get($result, 'bindings');
            $fields = array_get($result, 'fields');
            $fields = (empty($fields)) ? '*' : $fields;

            $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters, true);

            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->dbConn->table($table);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            if (!empty($parsed)) {
                $builder->update($parsed);
            }

            $results = $this->runQuery($table, $fields, $builder, $bindings, $extras);

            if (!empty($relatedInfo)) {
                // update related info
                foreach ($results as $row) {
                    $id = static::checkForIds($row, $idsInfo, $extras);
                    $this->updatePostRelations($table, array_merge($row, $record), $relatedInfo, $allowRelatedDelete);
                }
                // get latest with related changes if requested
                if (!empty($related)) {
                    $results = $this->runQuery($table, $fields, $builder, $bindings, $extras);
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
            $builder = $this->dbConn->table($table);
            $ssFilters = array_get($extras, 'ss_filters');
            $params = [];
            $serverFilter = $this->buildQueryStringFromData($ssFilters);
            if (!empty($serverFilter)) {
                Session::replaceLookups($serverFilter);
                $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
                $builder->whereRaw($filterString, $params);
                $builder->delete();
            } else {
                $builder->truncate();
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

        $idFields = array_get($extras, ApiOptions::ID_FIELD);
        $idTypes = array_get($extras, ApiOptions::ID_TYPE);
        $fields = array_get($extras, ApiOptions::FIELDS);
        $ssFilters = array_get($extras, 'ss_filters');

        try {
            $fieldsInfo = $this->getFieldsInfo($table);
            /*$idsInfo = */
            $this->getIdsInfo($table, $fieldsInfo, $idFields, $idTypes);
            $fields = (empty($fields)) ? $idFields : $fields;
            $result = $this->parseFieldsForSqlSelect($fields, $fieldsInfo);
            $bindings = array_get($result, 'bindings');
            $fields = array_get($result, 'fields');
            $fields = (empty($fields)) ? '*' : $fields;

            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->dbConn->table($table);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            $results = $this->runQuery($table, $fields, $builder, $bindings, $extras);

            $builder->delete();

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
        $fields = array_get($extras, ApiOptions::FIELDS);
        $ssFilters = array_get($extras, 'ss_filters');

        try {
            $fieldsInfo = $this->getFieldsInfo($table);
            $result = $this->parseFieldsForSqlSelect($fields, $fieldsInfo);
            $bindings = array_get($result, 'bindings');
            $fields = array_get($result, 'fields');
            $fields = (empty($fields)) ? '*' : $fields;

            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->dbConn->table($table);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            return $this->runQuery($table, $fields, $builder, $bindings, $extras);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to retrieve records from '$table'.\n{$ex->getMessage()}");
        }
    }

    // Helper methods

    protected function runQuery($table, $select, Builder $builder, $bind_columns, $extras)
    {
        $schema = $this->schema->getTable($table);
        if (!$schema) {
            throw new NotFoundException("Table '$table' does not exist in the database.");
        }

        $order = array_get($extras, ApiOptions::ORDER);
        $group = array_get($extras, ApiOptions::GROUP);
        $limit = intval(array_get($extras, ApiOptions::LIMIT, 0));
        $offset = intval(array_get($extras, ApiOptions::OFFSET, 0));
        $includeCount = Scalar::boolval(array_get($extras, ApiOptions::INCLUDE_COUNT));
        $related = array_get($extras, ApiOptions::RELATED);
        /** @type RelationSchema[] $availableRelations */
        $availableRelations = $schema->getRelations(true);
        $maxAllowed = static::getMaxRecordsReturnedLimit();
        $needLimit = false;

        // see if we need to add anymore fields to select for related retrieval
        if (('*' !== $select) && !empty($availableRelations) && (!empty($related) || $schema->fetchRequiresRelations)) {
            foreach ($availableRelations as $relation) {
                if (false === array_search($relation->field, $select)) {
                    $select[] = $relation->field;
                }
            }
        }

        // use query builder
        $builder->select($select);
        if (!empty($order)) {
            $builder->orderBy($order);
        }
        if (!empty($group)) {
            $builder->groupBy($group);
        }
        if (($limit < 1) || ($limit > $maxAllowed)) {
            // impose a limit to protect server
            $limit = $maxAllowed;
            $needLimit = true;
        }

        // count total records
        $count = ($includeCount || $needLimit) ? $builder->count() : 0;

        $builder->take($limit);
        $builder->skip($offset);

        $result = $builder->get();
        $data = [];
        $row = 0;
        foreach ($result as $record) {
            $temp = (array)$record;
            foreach ($bind_columns as $binding) {
                $name = array_get($binding, 'name');
                $type = array_get($binding, 'php_type');
                if (isset($temp[$name])) {
                    $temp[$name] = $this->schema->formatValue($temp[$name], $type);
                }
            }

            $data[$row++] = $temp;
        }

        $meta = [];
        if ($includeCount || $needLimit) {
            if ($includeCount || $count > $maxAllowed) {
                $meta['count'] = $count;
            }
            if (($count - $offset) > $limit) {
                $meta['next'] = $offset + $limit;
            }
        }

        if (Scalar::boolval(array_get($extras, ApiOptions::INCLUDE_SCHEMA))) {
            try {
                $meta['schema'] = $schema->toArray(true);
            } catch (RestException $ex) {
                throw $ex;
            } catch (\Exception $ex) {
                throw new InternalServerErrorException("Error describing database table '$table'.\n" .
                    $ex->getMessage(), $ex->getCode());
            }
        }

        if (!empty($related) || $schema->fetchRequiresRelations) {
            if (!empty($availableRelations)) {
                foreach ($data as $key => $temp) {
                    $data[$key] = $this->retrieveRelatedRecords($schema, $availableRelations, $related, $temp);
                }
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
     * @return ColumnSchema[]
     * @throws \Exception
     */
    protected function getFieldsInfo($table_name)
    {
        $table = $this->schema->getTable($table_name);
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
     * @return RelationSchema[]
     * @throws \Exception
     */
    protected function describeTableRelated($table_name)
    {
        $table = $this->schema->getTable($table_name);
        if (!$table) {
            throw new NotFoundException("Table '$table_name' does not exist in the database.");
        }

        return $table->getRelations(true);
    }

    /**
     * Take in a ANSI SQL filter string (WHERE clause)
     * or our generic NoSQL filter array or partial record
     * and parse it to the service's native filter criteria.
     * The filter string can have substitution parameters such as
     * ':name', in which case an associative array is expected,
     * for value substitution.
     *
     * @param \Illuminate\Database\Query\Builder $builder
     * @param string | array                     $filter       SQL WHERE clause filter string
     * @param array                              $params       Array of substitution values
     * @param array                              $ss_filters   Server-side filters to apply
     * @param array                              $avail_fields All available fields for the table
     *
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    protected function convertFilterToNative(
        Builder $builder,
        $filter,
        $params = [],
        $ss_filters = [],
        $avail_fields = []
    ){
        // interpret any parameter values as lookups
        $params = (is_array($params) ? static::interpretRecordValues($params) : []);
        $serverFilter = $this->buildQueryStringFromData($ss_filters);

        $outParams = [];
        if (empty($filter)) {
            $filter = $serverFilter;
        } elseif (is_string($filter)) {
            if (!empty($serverFilter)) {
                $filter = '(' . $filter . ') ' . DbLogicalOperators::AND_STR . ' (' . $serverFilter . ')';
            }
        } elseif (is_array($filter)) {
            // todo parse client filter?
            $filter = '';
            if (!empty($serverFilter)) {
                $filter = '(' . $filter . ') ' . DbLogicalOperators::AND_STR . ' (' . $serverFilter . ')';
            }
        }

        Session::replaceLookups($filter);
        $filterString = $this->parseFilterString($filter, $outParams, $avail_fields, $params);
        if (!empty($filterString)) {
            $builder->whereRaw($filterString, $outParams);
        }
    }

    /**
     * @param string         $filter
     * @param array          $out_params
     * @param ColumnSchema[] $fields_info
     * @param array          $in_params
     *
     * @return string
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseFilterString($filter, array &$out_params, $fields_info, array $in_params = [])
    {
        if (empty($filter)) {
            return null;
        }

        $filter = trim($filter);
        // todo use smarter regex
        // handle logical operators first
        $logicalOperators = DbLogicalOperators::getDefinedConstants();
        foreach ($logicalOperators as $logicalOp) {
            if (DbLogicalOperators::NOT_STR === $logicalOp) {
                // NOT(a = 1)  or NOT (a = 1)format
                if ((0 === stripos($filter, $logicalOp . ' (')) || (0 === stripos($filter, $logicalOp . '('))) {
                    $parts = trim(substr($filter, 3));
                    $parts = $this->parseFilterString($parts, $out_params, $fields_info, $in_params);

                    return static::localizeOperator($logicalOp) . $parts;
                }
            } else {
                // (a = 1) AND (b = 2) format or (a = 1)AND(b = 2) format
                $filter = str_ireplace(')' . $logicalOp . '(', ') ' . $logicalOp . ' (', $filter);
                $paddedOp = ') ' . $logicalOp . ' (';
                if (false !== $pos = stripos($filter, $paddedOp)) {
                    $left = trim(substr($filter, 0, $pos)) . ')'; // add back right )
                    $right = '(' . trim(substr($filter, $pos + strlen($paddedOp))); // adding back left (
                    $left = $this->parseFilterString($left, $out_params, $fields_info, $in_params);
                    $right = $this->parseFilterString($right, $out_params, $fields_info, $in_params);

                    return $left . ' ' . static::localizeOperator($logicalOp) . ' ' . $right;
                }
            }
        }

        $wrap = false;
        if ((0 === strpos($filter, '(')) && ((strlen($filter) - 1) === strrpos($filter, ')'))) {
            // remove unnecessary wrapping ()
            $filter = substr($filter, 1, -1);
            $wrap = true;
        }

        // Some scenarios leave extra parens dangling
        $pure = trim($filter, '()');
        $pieces = explode($pure, $filter);
        $leftParen = (!empty($pieces[0]) ? $pieces[0] : null);
        $rightParen = (!empty($pieces[1]) ? $pieces[1] : null);
        $filter = $pure;

        // the rest should be comparison operators
        // Note: order matters here!
        $sqlOperators = DbComparisonOperators::getParsingOrder();
        foreach ($sqlOperators as $sqlOp) {
            $paddedOp = static::padOperator($sqlOp);
            if (false !== $pos = stripos($filter, $paddedOp)) {
                $field = trim(substr($filter, 0, $pos));
                $negate = false;
                if (false !== strpos($field, ' ')) {
                    $parts = explode(' ', $field);
                    $partsCount = count($parts);
                    if (($partsCount > 1) &&
                        (0 === strcasecmp($parts[$partsCount - 1], trim(DbLogicalOperators::NOT_STR)))
                    ) {
                        // negation on left side of operator
                        array_pop($parts);
                        $field = implode(' ', $parts);
                        $negate = true;
                    }
                }
                /** @type ColumnSchema $info */
                if (null === $info = array_get($fields_info, strtolower($field))) {
                    // This could be SQL injection attempt or bad field
                    throw new BadRequestException("Invalid or unparsable field in filter request: '$field'");
                }

                // make sure we haven't chopped off right side too much
                $value = trim(substr($filter, $pos + strlen($paddedOp)));
                if ((0 !== strpos($value, "'")) &&
                    (0 !== $lpc = substr_count($value, '(')) &&
                    ($lpc !== $rpc = substr_count($value, ')'))
                ) {
                    // add back to value from right
                    $parenPad = str_repeat(')', $lpc - $rpc);
                    $value .= $parenPad;
                    $rightParen = preg_replace('/\)/', '', $rightParen, $lpc - $rpc);
                }
                if (DbComparisonOperators::requiresValueList($sqlOp)) {
                    if ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                        // remove wrapping ()
                        $value = substr($value, 1, -1);
                        $parsed = [];
                        foreach (explode(',', $value) as $each) {
                            $parsed[] = $this->parseFilterValue(trim($each), $info, $out_params, $in_params);
                        }
                        $value = '(' . implode(',', $parsed) . ')';
                    } else {
                        throw new BadRequestException('Filter value lists must be wrapped in parentheses.');
                    }
                } elseif (DbComparisonOperators::requiresNoValue($sqlOp)) {
                    $value = null;
                } else {
                    static::modifyValueByOperator($sqlOp, $value);
                    $value = $this->parseFilterValue($value, $info, $out_params, $in_params);
                }

                $sqlOp = static::localizeOperator($sqlOp);
                if ($negate) {
                    $sqlOp = DbLogicalOperators::NOT_STR . ' ' . $sqlOp;
                }

                $out = $info->parseFieldForFilter(true) . " $sqlOp";
                $out .= (isset($value) ? " $value" : null);
                if ($leftParen) {
                    $out = $leftParen . $out;
                }
                if ($rightParen) {
                    $out .= $rightParen;
                }

                return ($wrap ? '(' . $out . ')' : $out);
            }
        }

        // This could be SQL injection attempt or unsupported filter arrangement
        throw new BadRequestException('Invalid or unparsable filter request.');
    }

    /**
     * @param mixed        $value
     * @param ColumnSchema $info
     * @param array        $out_params
     * @param array        $in_params
     *
     * @return int|null|string
     * @throws BadRequestException
     */
    protected function parseFilterValue($value, ColumnSchema $info, array &$out_params, array $in_params = [])
    {
        // if a named replacement parameter, un-name it because Laravel can't handle named parameters
        if (is_array($in_params) && (0 === strpos($value, ':'))) {
            if (array_key_exists($value, $in_params)) {
                $value = $in_params[$value];
            }
        }

        // remove quoting on strings if used, i.e. 1.x required them
        if (is_string($value)) {

            if ((0 === strcmp("'" . trim($value, "'") . "'", $value)) ||
                (0 === strcmp('"' . trim($value, '"') . '"', $value))
            ) {
                $value = substr($value, 1, -1);
            } elseif ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                // function call
                return $value;
            }
        }
        // if not already a replacement parameter, evaluate it
        try {
            $value = $this->schema->parseValueForSet($value, $info);
        } catch (ForbiddenException $ex) {
            // need to prop this up?
        }

        switch ($cnvType = $info->determinePhpConversionType($info->type)) {
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
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;
            case 'date':
                $cfgFormat = Config::get('df.db_date_format');
                $outFormat = 'Y-m-d';
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;
            case 'datetime':
                $cfgFormat = Config::get('df.db_datetime_format');
                $outFormat = 'Y-m-d H:i:s';
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;
            case 'timestamp':
                $cfgFormat = Config::get('df.db_timestamp_format');
                $outFormat = 'Y-m-d H:i:s';
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;

            default:
                break;
        }

        $out_params[] = $value;
        $value = '?';

        return $value;
    }

    /**
     * @throws \Exception
     */
    protected function getCurrentTimestamp()
    {
        return $this->schema->getTimestampForSet();
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
            $value = $this->schema->parseValueForSet($value, $field_info);

            switch ($cnvType = $field_info->determinePhpConversionType($field_info->type)) {
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
                    $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                    break;
                case 'date':
                    $cfgFormat = Config::get('df.db_date_format');
                    $outFormat = 'Y-m-d';
                    $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                    break;
                case 'datetime':
                    $cfgFormat = Config::get('df.db_datetime_format');
                    $outFormat = 'Y-m-d H:i:s';
                    $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                    break;
                case 'timestamp':
                    $cfgFormat = Config::get('df.db_timestamp_format');
                    $outFormat = 'Y-m-d H:i:s';
                    $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
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
     * @param array            $record Record containing relationships by name if any
     * @param RelationSchema[] $relations
     *
     * @throws InternalServerErrorException
     * @return void
     */
    protected function updatePreRelations(&$record, $relations)
    {
        $record = array_change_key_case($record, CASE_LOWER);
        foreach ($relations as $name => $relationInfo) {
            if (!empty($relatedRecords = array_get($record, $name))) {
                switch ($relationInfo->type) {
                    case RelationSchema::BELONGS_TO:
                        $this->updateBelongsTo($relationInfo, $record, $relatedRecords);
                        unset($record[$name]);
                        break;
                }
            }
        }
    }

    /**
     * @param string           $table
     * @param array            $record Record containing relationships by name if any
     * @param RelationSchema[] $relations
     * @param bool             $allow_delete
     *
     * @throws InternalServerErrorException
     * @return void
     */
    protected function updatePostRelations($table, $record, $relations, $allow_delete = false)
    {
        $schema = $this->getTableSchema(null, $table);
        $record = array_change_key_case($record, CASE_LOWER);
        foreach ($relations as $name => $relationInfo) {
            if (array_key_exists($name, $record)) {
                $relatedRecords = $record[$name];
                unset($record[$name]);
                switch ($relationInfo->type) {
                    case RelationSchema::HAS_MANY:
                        $this->assignManyToOne(
                            $schema,
                            $record,
                            $relationInfo,
                            $relatedRecords,
                            $allow_delete
                        );
                        break;
                    case RelationSchema::MANY_MANY:
                        $this->assignManyToOneByJunction(
                            $schema,
                            $record,
                            $relationInfo,
                            $relatedRecords
                        );
                        break;
                }
            }
        }
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
                $bindArray[] = $fieldInfo->getPdoBinding();
                $outArray[] = $fieldInfo->parseFieldForSelect(false);
            }
        } else {
            foreach ($avail_fields as $fieldInfo) {
                if ($fieldInfo->isAggregate()) {
                    continue;
                }
                $bindArray[] = $fieldInfo->getPdoBinding();
                $outArray[] = $fieldInfo->parseFieldForSelect(false);
            }
        }

        return ['fields' => $outArray, 'bindings' => $bindArray];
    }

    // generic assignments

    /**
     * @param TableSchema      $schema
     * @param RelationSchema[] $relations
     * @param string|array     $requests
     * @param array            $data
     *
     * @throws InternalServerErrorException
     * @throws BadRequestException
     * @return array
     */
    protected function retrieveRelatedRecords(TableSchema $schema, $relations, $requests, $data)
    {
        $relatedData = [];
        $relatedExtras = [ApiOptions::LIMIT => static::getMaxRecordsReturnedLimit(), ApiOptions::FIELDS => '*'];
        foreach ($relations as $key => $relation) {
            if (empty($relation)) {
                throw new BadRequestException("Empty relationship found.");
            }

            if (is_array($requests) && array_key_exists($key, $requests)) {
                $relatedData[$relation->getName(true)] =
                    $this->retrieveRelationRecords($schema, $relation, $data, $requests[$key]);
            } elseif (('*' == $requests) || $relation->alwaysFetch) {
                $relatedData[$relation->getName(true)] =
                    $this->retrieveRelationRecords($schema, $relation, $data, $relatedExtras);
            }
        }

        return array_merge($data, $relatedData);
    }

    /**
     * @param string $serviceName
     * @param string $resource
     * @param null   $params
     *
     * @return mixed|null
     * @throws \DreamFactory\Core\Exceptions\ForbiddenException
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     * @throws \DreamFactory\Core\Exceptions\RestException
     */
    protected function retrieveVirtualRecords($serviceName, $resource, $params = null)
    {
        if (empty($serviceName)) {
            return null;
        }

        $result = null;
        $params = (is_array($params) ? $params : []);

        $request = new Service2ServiceRequest(Verbs::GET, $params);

        //  Now set the request object and go...
        $service = ServiceHandler::getService($serviceName);
        $response = $service->handleRequest($request, $resource);
        $content = $response->getContent();
        $status = $response->getStatusCode();

        if (empty($content)) {
            // No content specified.
            return null;
        }

        switch ($status) {
            case 200:
                if (isset($content)) {
                    return (isset($content['resource']) ? $content['resource'] : $content);
                }

                throw new InternalServerErrorException('Virtual query succeeded but returned invalid format.');
                break;
            default:
                if (isset($content, $content['error'])) {
                    $error = $content['error'];
                    extract($error);
                    /** @noinspection PhpUndefinedVariableInspection */
                    throw new RestException($status, $message, $code);
                }

                throw new RestException($status, 'Virtual query failed but returned invalid format.');
        }
    }

    /**
     * @param string     $serviceName
     * @param string     $resource
     * @param string     $verb
     * @param null|array $records
     * @param null|array $params
     *
     * @return mixed|null
     * @throws \DreamFactory\Core\Exceptions\ForbiddenException
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     * @throws \DreamFactory\Core\Exceptions\RestException
     * @internal param $path
     */
    protected function handleVirtualRecords($serviceName, $resource, $verb, $records = null, $params = null)
    {
        if (empty($serviceName)) {
            return null;
        }

        $result = null;
        $params = (is_array($params) ? $params : []);

        $request = new Service2ServiceRequest($verb, $params);
        if (!empty($records)) {
            $records = ResourcesWrapper::wrapResources($records);
            $request->setContent($records);
        }

        //  Now set the request object and go...
        $service = ServiceHandler::getService($serviceName);
        $response = $service->handleRequest($request, $resource);
        $content = $response->getContent();
        $status = $response->getStatusCode();

        if (empty($content)) {
            // No content specified.
            return null;
        }

        switch ($status) {
            case 200:
            case 201:
                if (isset($content)) {
                    return (isset($content['resource']) ? $content['resource'] : $content);
                }

                throw new InternalServerErrorException('Virtual query succeeded but returned invalid format.');
                break;
            default:
                if (isset($content, $content['error'])) {
                    $error = $content['error'];
                    extract($error);
                    /** @noinspection PhpUndefinedVariableInspection */
                    throw new RestException($status, $message, $code);
                }

                throw new RestException($status, 'Virtual query failed but returned invalid format.');
        }
    }

    protected function getTableSchema($service, $table)
    {
        if (!empty($service) && ($service !== $this->getServiceName())) {
            // non-native service relation, go get it
            if (!empty($result = $this->retrieveVirtualRecords($service, '_schema/' . $table))) {
                return new TableSchema($result);
            }
        } else {
            return $this->schema->getTable($table);
        }

        return null;
    }

    /**
     * @param string $service
     * @param string $table
     * @param array  $fields
     *
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     * @throws \DreamFactory\Core\Exceptions\RestException
     */
    protected function replaceWithAliases($service, &$table, array &$fields)
    {
        if (!empty($refSchema = $this->getTableSchema($service, $table))) {
            $table = $refSchema->getName(true);
            foreach ($fields as &$field) {
                if (!empty($temp = $refSchema->getColumn($field))) {
                    $field = $temp->getName(true);
                }
            }
        }
    }

    /**
     * @param TableSchema    $schema
     * @param RelationSchema $relation
     * @param array          $data
     * @param array          $extras
     *
     * @return array|null
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     * @throws \DreamFactory\Core\Exceptions\NotFoundException
     * @throws \DreamFactory\Core\Exceptions\RestException
     * @throws \Exception
     */
    protected function retrieveRelationRecords(TableSchema $schema, RelationSchema $relation, $data, $extras)
    {
        $localFieldInfo = $schema->getColumn($relation->field);
        $localField = $localFieldInfo->getName(true);
        $fieldVal = array_get($data, $localField);
        if (empty($fieldVal)) {
            switch ($relation->type) {
                case RelationSchema::BELONGS_TO:
                    return null;
                default:
                    return [];
            }
        }

        $extras = (is_array($extras) ? $extras : []);
        switch ($relation->type) {
            case RelationSchema::BELONGS_TO:
            case RelationSchema::HAS_MANY:
                $refService = ($relation->isForeignService ? $relation->refService : $this->getServiceName());
                $refSchema = $this->getTableSchema($relation->refService, $relation->refTable);
                $refTable = $refSchema->getName(true);
                if (empty($refField = $refSchema->getColumn($relation->refFields))) {
                    throw new InternalServerErrorException("Incorrect relationship configuration detected. Field '{$relation->refFields} not found.");
                }

                // check for access
                Session::checkServicePermission(Verbs::GET, $refService, '_table/' . $refTable);

                // Get records
                $filterVal = ('string' === gettype($fieldVal)) ? "'$fieldVal'" : $fieldVal;
                $extras[ApiOptions::FILTER] = '(' . $refField->getName(true) . ' = ' . $filterVal . ')';
                $relatedRecords = $this->retrieveVirtualRecords($refService, '_table/' . $refTable, $extras);
                if (RelationSchema::BELONGS_TO === $relation->type) {
                    return (!empty($relatedRecords) ? array_get($relatedRecords, 0) : null);
                }

                return $relatedRecords;
                break;
            case RelationSchema::MANY_MANY:
                $junctionService =
                    ($relation->isForeignJunctionService ? $relation->junctionService : $this->getServiceName());
                $junctionSchema = $this->getTableSchema($junctionService, $relation->junctionTable);
                $junctionTable = $junctionSchema->getName(true);
                $junctionField = $junctionSchema->getColumn($relation->junctionField);
                $junctionRefField = $junctionSchema->getColumn($relation->junctionRefField);
                if (empty($junctionTable) ||
                    empty($junctionField) ||
                    empty($junctionRefField)
                ) {
                    throw new InternalServerErrorException('Many to many relationship not configured properly.');
                }

                // check for access
                Session::checkServicePermission(Verbs::GET, $junctionService, '_table/' . $junctionTable);

                // Get records
                $filterVal = ('string' === gettype($fieldVal)) ? "'$fieldVal'" : $fieldVal;
                $filter = '(' . $junctionField->getName(true) . ' = ' . $filterVal . ')';
                $filter .= static::padOperator(DbLogicalOperators::AND_STR);
                $filter .= '(' . $junctionRefField->getName(true) . ' ' . DbComparisonOperators::IS_NOT_NULL . ')';
                $temp = [ApiOptions::FILTER => $filter, ApiOptions::FIELDS => $junctionRefField->getName(true)];
                $joinData = $this->retrieveVirtualRecords($junctionService, '_table/' . $junctionTable, $temp);
                if (empty($joinData)) {
                    return [];
                }

                $relatedIds = [];
                foreach ($joinData as $record) {
                    if (null !== $rightValue = array_get($record, $junctionRefField->getName(true))) {
                        $relatedIds[] = $rightValue;
                    }
                }
                if (!empty($relatedIds)) {
                    $refService = ($relation->isForeignService ? $relation->refService : $this->getServiceName());
                    $refSchema = $this->getTableSchema($relation->refService, $relation->refTable);
                    $refTable = $refSchema->getName(true);
                    if (empty($refField = $refSchema->getColumn($relation->refFields))) {
                        throw new InternalServerErrorException("Incorrect relationship configuration detected. Field '{$relation->refFields} not found.");
                    }

                    // check for access
                    Session::checkServicePermission(Verbs::GET, $refService, '_table/' . $refTable);

                    // Get records
                    if (count($relatedIds) > 1) {
                        $filter = $refField->getName(true) . ' IN (' . implode(',', $relatedIds) . ')';
                    } else {
                        $filter = $refField->getName(true) . ' = ' . $relatedIds[0];
                    }
                    $extras[ApiOptions::FILTER] = $filter;
                    $relatedRecords = $this->retrieveVirtualRecords($refService, '_table/' . $refTable, $extras);

                    return $relatedRecords;
                }
                break;
            default:
                throw new InternalServerErrorException('Invalid relationship type detected.');
                break;
        }

        return null;
    }

    /**
     * @param RelationSchema $relation
     * @param array          $record
     * @param array          $parent
     *
     * @throws BadRequestException
     * @return void
     */
    protected function updateBelongsTo(RelationSchema $relation, &$record, $parent)
    {
        try {
            $refService = ($relation->isForeignService ? $relation->refService : $this->getServiceName());
            $refSchema = $this->getTableSchema($refService, $relation->refTable);
            $refTable = $refSchema->getName(true);
            if (empty($refField = $refSchema->getColumn($relation->refFields))) {
                throw new InternalServerErrorException("Incorrect relationship configuration detected. Field '{$relation->refFields} not found.");
            }

            if (is_array($refSchema->primaryKey)) {
                if (1 < count($refSchema->primaryKey)) {
                    // todo How to handle multiple primary keys?
                    throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                } else {
                    $pkField = $refSchema->primaryKey[0];
                }
            } else {
                $pkField = $refSchema->primaryKey;
            }
            $pkField = $refSchema->getColumn($pkField);

            $pkAutoSet = $pkField->autoIncrement;
            $pkFieldAlias = $pkField->getName(true);

            // figure out which batch each related record falls into
            $insertMany = [];
            $updateMany = [];
            $id = array_get($parent, $pkFieldAlias);
            if (empty($id)) {
                if (!$pkAutoSet) {
                    throw new BadRequestException("Related record has no primary key value for '$pkFieldAlias'.");
                }

                // create new parent record
                $insertMany[] = $parent;
            } else {
                // update or insert a parent
                // check for access
                Session::checkServicePermission(Verbs::GET, $refService, '_table/' . $refTable);

                // Get records
                $filterVal = ('string' === gettype($id)) ? "'$id'" : $id;
                $temp = [ApiOptions::FILTER => "$pkFieldAlias = $filterVal"];
                $matchIds = $this->retrieveVirtualRecords($refService, '_table/' . $refTable, $temp);

                if ($found = static::findRecordByNameValue($matchIds, $pkFieldAlias, $id)) {
                    $updateMany[] = $parent;
                } else {
                    $insertMany[] = $parent;
                }
            }

            if (!empty($insertMany)) {
                if (!empty($newIds = $this->createForeignRecords($refService, $refSchema, $insertMany))) {
                    if ($relation->refFields === $pkFieldAlias) {
                        $record[$relation->field] = array_get(reset($newIds), $pkFieldAlias);
                    } else {
                        $record[$relation->field] = array_get(reset($insertMany), $relation->refFields);
                    }
                }
            }

            if (!empty($updateMany)) {
                $this->updateForeignRecords($refService, $refSchema, $pkField, $updateMany);
            }
        } catch (\Exception $ex) {
            throw new BadRequestException("Failed to update belongs-to assignment.\n{$ex->getMessage()}");
        }
    }

    /**
     * @param TableSchema    $one_table
     * @param array          $one_record
     * @param RelationSchema $relation
     * @param array          $many_records
     * @param bool           $allow_delete
     *
     * @throws BadRequestException
     * @return void
     */
    protected function assignManyToOne(
        TableSchema $one_table,
        $one_record,
        RelationSchema $relation,
        $many_records = [],
        $allow_delete = false
    ){
        // update currently only supports one id field
        if (empty($one_id = array_get($one_record, $relation->field))) {
            throw new BadRequestException("The {$one_table->getName(true)} id can not be empty.");
        }

        try {
            $refService = ($relation->isForeignService ? $relation->refService : $this->getServiceName());
            $refSchema = $this->getTableSchema($refService, $relation->refTable);
            $refTable = $refSchema->getName(true);
            if (empty($refField = $refSchema->getColumn($relation->refFields))) {
                throw new InternalServerErrorException("Incorrect relationship configuration detected. Field '{$relation->refFields} not found.");
            }

            if (is_array($refSchema->primaryKey)) {
                if (1 < count($refSchema->primaryKey)) {
                    // todo How to handle multiple primary keys?
                    throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                } else {
                    $pkField = $refSchema->primaryKey[0];
                }
            } else {
                $pkField = $refSchema->primaryKey;
            }
            $pkField = $refSchema->getColumn($pkField);

            $pkAutoSet = $pkField->autoIncrement;
            $pkFieldAlias = $pkField->getName(true);
            $refFieldAlias = $refField->getName(true);
            $deleteRelated = (!$refField->allowNull && $allow_delete);

            // figure out which batch each related record falls into
            $relateMany = [];
            $disownMany = [];
            $insertMany = [];
            $updateMany = [];
            $upsertMany = [];
            $deleteMany = [];
            foreach ($many_records as $item) {
                $id = array_get($item, $pkFieldAlias);
                if (empty($id)) {
                    if (!$pkAutoSet) {
                        throw new BadRequestException("Related record has no primary key value for '$pkFieldAlias'.");
                    }

                    // create new child record
                    $item[$refFieldAlias] = $one_id; // assign relationship
                    $insertMany[] = $item;
                } else {
                    if (array_key_exists($refFieldAlias, $item)) {
                        if (null == array_get($item, $refFieldAlias)) {
                            // disown this child or delete them
                            if ($deleteRelated) {
                                $deleteMany[] = $id;
                            } elseif (count($item) > 1) {
                                $item[$refFieldAlias] = null; // assign relationship
                                $updateMany[] = $item;
                            } else {
                                $disownMany[] = $id;
                            }

                            continue;
                        }
                    }

                    // update or upsert this child
                    if (count($item) > 1) {
                        $item[$refFieldAlias] = $one_id; // assign relationship
                        if ($pkAutoSet) {
                            $updateMany[] = $item;
                        } else {
                            $upsertMany[$id] = $item;
                        }
                    } else {
                        $relateMany[] = $id;
                    }
                }
            }

            // resolve any upsert situations
            if (!empty($upsertMany)) {
                // check for access
                Session::checkServicePermission(Verbs::GET, $refService, '_table/' . $refTable);

                // Get records
                $checkIds = array_keys($upsertMany);
                if (count($checkIds) > 1) {
                    $filter = $pkFieldAlias . ' IN (' . implode(',', $checkIds) . ')';
                } else {
                    $filter = $pkFieldAlias . ' = ' . $checkIds[0];
                }
                $temp = [ApiOptions::FILTER => $filter];
                $matchIds = $this->retrieveVirtualRecords($refService, '_table/' . $refTable, $temp);

                foreach ($upsertMany as $uId => $record) {
                    if ($found = static::findRecordByNameValue($matchIds, $pkFieldAlias, $uId)) {
                        $updateMany[] = $record;
                    } else {
                        $insertMany[] = $record;
                    }
                }
            }

            // Now handle the batches
            if (!empty($insertMany)) {
                // create new children
                $this->createForeignRecords($refService, $refSchema, $insertMany);
            }

            if (!empty($deleteMany)) {
                // destroy linked children that can't stand alone - sounds sinister
                $this->deleteForeignRecords($refService, $refSchema, $pkField, $deleteMany);
            }

            if (!empty($updateMany)) {
                $this->updateForeignRecords($refService, $refSchema, $pkField, $updateMany);
            }

            if (!empty($relateMany)) {
                // adopt/relate/link unlinked children
                $updates = [$refFieldAlias => $one_id];
                $this->updateForeignRecordsByIds($refService, $refSchema, $pkField, $relateMany, $updates);
            }

            if (!empty($disownMany)) {
                // disown/un-relate/unlink linked children
                $updates = [$refFieldAlias => null];
                $this->updateForeignRecordsByIds($refService, $refSchema, $pkField, $disownMany, $updates);
            }
        } catch (\Exception $ex) {
            throw new BadRequestException("Failed to update many to one assignment.\n{$ex->getMessage()}");
        }
    }

    protected function createForeignRecords($service, TableSchema $schema, $records)
    {
        // do we have permission to do so?
        Session::checkServicePermission(Verbs::POST, $service, '_table/' . $schema->getName(true));
        if (!empty($service) && ($service !== $this->getServiceName())) {
            $newIds = $this->handleVirtualRecords($service, '_table/' . $schema->getName(true), Verbs::POST, $records);
        } else {
            $tableName = $schema->getName();
            $builder = $this->dbConn->table($tableName);
            $fields = $schema->getColumns(true);
            $ssFilters = Session::getServiceFilters(Verbs::POST, $service, $schema->getName(true));
            $newIds = [];
            foreach ($records as $record) {
                $parsed = $this->parseRecord($record, $fields, $ssFilters);
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                $newIds[] = (int)$builder->insertGetId($parsed, $schema->primaryKey);
            }
        }

        return $newIds;
    }

    protected function updateForeignRecords(
        $service,
        TableSchema $schema,
        ColumnSchema $linkerField,
        $records
    ){
        // do we have permission to do so?
        Session::checkServicePermission(Verbs::PUT, $service, '_table/' . $schema->getName(true));
        if (!empty($service) && ($service !== $this->getServiceName())) {
            $this->handleVirtualRecords($service, '_table/' . $schema->getName(true), Verbs::PATCH, $records);
        } else {
            $fields = $schema->getColumns(true);
            $ssFilters = Session::getServiceFilters(Verbs::PUT, $service, $schema->getName(true));
            // update existing and adopt new children
            foreach ($records as $record) {
                $pk = array_get($record, $linkerField->getName(true));
                $parsed = $this->parseRecord($record, $fields, $ssFilters, true);
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found for foreign link updates.');
                }

                $builder = $this->dbConn->table($schema->getName());
                $builder->where($linkerField->name, $pk);
                $serverFilter = $this->buildQueryStringFromData($ssFilters);
                if (!empty($serverFilter)) {
                    Session::replaceLookups($serverFilter);
                    $params = [];
                    $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
                    $builder->whereRaw($filterString, $params);
                }

                $rows = $builder->update($parsed);
                if (0 >= $rows) {
//            throw new NotFoundException( 'No foreign linked records were found using the given identifiers.' );
                }
            }
        }
    }

    protected function updateForeignRecordsByIds(
        $service,
        TableSchema $schema,
        ColumnSchema $linkerField,
        $linkerIds,
        $record
    ){
        // do we have permission to do so?
        Session::checkServicePermission(Verbs::PUT, $service, '_table/' . $schema->getName(true));
        if (!empty($service) && ($service !== $this->getServiceName())) {
            $temp = [ApiOptions::IDS => $linkerIds, ApiOptions::ID_FIELD => $linkerField->getName(true)];
            $this->handleVirtualRecords($service, '_table/' . $schema->getName(true), Verbs::PATCH, $record, $temp);
        } else {
            $fields = $schema->getColumns(true);
            $ssFilters = Session::getServiceFilters(Verbs::PUT, $service, $schema->getName(true));
            $parsed = $this->parseRecord($record, $fields, $ssFilters, true);
            if (empty($parsed)) {
                throw new BadRequestException('No valid fields were found for foreign link updates.');
            }
            $builder = $this->dbConn->table($schema->getName());
            $builder->whereIn($linkerField->name, $linkerIds);
            $serverFilter = $this->buildQueryStringFromData($ssFilters);
            if (!empty($serverFilter)) {
                Session::replaceLookups($serverFilter);
                $params = [];
                $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
                $builder->whereRaw($filterString, $params);
            }

            $rows = $builder->update($parsed);
            if (0 >= $rows) {
//            throw new NotFoundException( 'No foreign linked records were found using the given identifiers.' );
            }
        }
    }

    protected function deleteForeignRecords(
        $service,
        TableSchema $schema,
        ColumnSchema $linkerField,
        $linkerIds,
        $addCondition = null
    ){
        // do we have permission to do so?
        Session::checkServicePermission(Verbs::DELETE, $service, '_table/' . $schema->getName(true));
        if (!empty($service) && ($service !== $this->getServiceName())) {
            if (!empty($addCondition) && is_array($addCondition)) {
                $filter = '(' . $linkerField->getName(true) . ' IN (' . implode(',', $$linkerIds) . '))';
                foreach ($addCondition as $key => $value) {
                    $column = $schema->getColumn($key);
                    $filter .= 'AND (' . $column->getName(true) . ' = ' . $value . ')';
                }
                $temp = [ApiOptions::FILTER => $filter];
            } else {
                $temp = [ApiOptions::IDS => $linkerIds, ApiOptions::ID_FIELD => $linkerField->getName(true)];
            }

            $this->handleVirtualRecords($service, '_table/' . $schema->getName(true), Verbs::DELETE, null, $temp);
        } else {
            $builder = $this->dbConn->table($schema->getName());
            $builder->whereIn($linkerField->name, $linkerIds);

            $ssFilters = Session::getServiceFilters(Verbs::DELETE, $service, $schema->getName(true));
            $serverFilter = $this->buildQueryStringFromData($ssFilters);
            if (!empty($serverFilter)) {
                Session::replaceLookups($serverFilter);
                $params = [];
                $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
                $builder->whereRaw($filterString, $params);
            }

            if (!empty($addCondition) && is_array($addCondition)) {
                foreach ($addCondition as $key => $value) {
                    $column = $schema->getColumn($key);
                    $builder->where($column->name, $value);
                }
            }

            $rows = $builder->delete();
            if (0 >= $rows) {
//            throw new NotFoundException( 'No foreign linked records were found using the given identifiers.' );
            }
        }
    }

    /**
     * @param TableSchema    $one_table
     * @param array          $one_record
     * @param RelationSchema $relation
     * @param array          $many_records
     *
     * @throws InternalServerErrorException
     * @throws BadRequestException
     * @return void
     */
    protected function assignManyToOneByJunction(
        TableSchema $one_table,
        $one_record,
        RelationSchema $relation,
        $many_records = []
    ){
        if (empty($one_id = array_get($one_record, $relation->field))) {
            throw new BadRequestException("The {$one_table->getName(true)} id can not be empty.");
        }

        try {
            if (is_array($one_table->primaryKey)) {
                if (1 !== count($one_table->primaryKey)) {
                    // todo How to handle multiple primary keys?
                    throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                } else {
                    $onePkFieldName = $one_table->primaryKey[0];
                }
            } else {
                $onePkFieldName = $one_table->primaryKey;
            }
            if (empty($onePkField = $one_table->getColumn($onePkFieldName))) {
                throw new InternalServerErrorException("Incorrect relationship configuration detected. Field '$onePkFieldName' not found.");
            }

            $refService = ($relation->isForeignService ? $relation->refService : $this->getServiceName());
            $refSchema = $this->getTableSchema($refService, $relation->refTable);
            $refTable = $refSchema->getName(true);
            if (empty($refField = $refSchema->getColumn($relation->refFields))) {
                throw new InternalServerErrorException("Incorrect relationship configuration detected. Field '{$relation->refFields} not found.");
            }
            if (is_array($refSchema->primaryKey)) {
                if (1 !== count($refSchema->primaryKey)) {
                    // todo How to handle multiple primary keys?
                    throw new NotImplementedException("Relating records with multiple field primary keys is not currently supported.");
                } else {
                    $refPkFieldName = $refSchema->primaryKey[0];
                }
            } else {
                $refPkFieldName = $refSchema->primaryKey;
            }
            if (empty($refPkField = $refSchema->getColumn($refPkFieldName))) {
                throw new InternalServerErrorException("Incorrect relationship configuration detected. Field '$refPkFieldName' not found.");
            }

            $junctionService =
                ($relation->isForeignJunctionService ? $relation->junctionService : $this->getServiceName());
            $junctionSchema = $this->getTableSchema($junctionService, $relation->junctionTable);
            $junctionTable = $junctionSchema->getName(true);
            if (empty($junctionField = $junctionSchema->getColumn($relation->junctionField))) {
                throw new InternalServerErrorException("Incorrect relationship configuration detected. Field '{$relation->junctionField} not found.");
            }
            if (empty($junctionRefField = $junctionSchema->getColumn($relation->junctionRefField))) {
                throw new InternalServerErrorException("Incorrect relationship configuration detected. Field '{$relation->junctionRefField} not found.");
            }

            // check for access
            Session::checkServicePermission(Verbs::GET, $junctionService, '_table/' . $junctionTable);

            // Get records
            $filter =
                $junctionField->getName(true) . " = $one_id AND (" . $junctionRefField->getName(true) . " IS NOT NULL)";
            $temp = [ApiOptions::FILTER => $filter, ApiOptions::FIELDS => $junctionRefField->getName(true)];

            $maps = $this->retrieveVirtualRecords($junctionService, '_table/' . $junctionTable, $temp);

            $createMap = []; // map records to create
            $deleteMap = []; // ids of 'many' records to delete from maps
            $insertMany = [];
            $updateMany = [];
            $upsertMany = [];

            $pkAutoSet = $refPkField->autoIncrement;
            $refPkFieldAlias = $refPkField->getName(true);
            foreach ($many_records as $item) {
                $id = array_get($item, $refPkFieldAlias);
                if (empty($id)) {
                    if (!$pkAutoSet) {
                        throw new BadRequestException("Related record has no primary key value for '$refPkFieldAlias'.");
                    }

                    // create new child record
                    $insertMany[] = $item;
                } else {
                    // pk fields exists, must be dealing with existing 'many' record
                    $oneLookup = $one_table->getName(true) . '.' . $onePkField->getName(true);
                    if (array_key_exists($oneLookup, $item)) {
                        if (null == array_get($item, $oneLookup)) {
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
                        if (array_get($map, $junctionRefField->getName(true)) == $id) {
                            continue 2; // got what we need from this one
                        }
                    }

                    $createMap[] = [$junctionRefField->getName(true) => $id, $junctionField->getName(true) => $one_id];
                }
            }

            // resolve any upsert situations
            if (!empty($upsertMany)) {
                // check for access
                Session::checkServicePermission(Verbs::GET, $refService, '_table/' . $refTable);

                // Get records
                $checkIds = array_keys($upsertMany);
                if (count($checkIds) > 1) {
                    $filter = $refPkFieldAlias . ' IN (' . implode(',', $checkIds) . ')';
                } else {
                    $filter = $refPkFieldAlias . ' = ' . $checkIds[0];
                }
                $temp = [ApiOptions::FILTER => $filter];
                $matchIds = $this->retrieveVirtualRecords($refService, '_table/' . $refTable, $temp);

                foreach ($upsertMany as $uId => $record) {
                    if ($found = static::findRecordByNameValue($matchIds, $refPkFieldAlias, $uId)) {
                        $updateMany[] = $record;
                    } else {
                        $insertMany[] = $record;
                    }
                }
            }

            if (!empty($insertMany)) {
                $refIds = $this->createForeignRecords($refService, $refSchema, $insertMany);
                // create new many records
                foreach ($refIds as $refId) {
                    if (!empty($refId)) {
                        $createMap[] =
                            [$junctionRefField->getName(true) => $refId, $junctionField->getName(true) => $one_id];
                    }
                }
            }

            if (!empty($updateMany)) {
                // update existing many records
                $this->updateForeignRecords($refService, $refSchema, $refPkField, $updateMany);
            }

            if (!empty($createMap)) {
                $this->createForeignRecords($junctionService, $junctionSchema, $createMap);
            }

            if (!empty($deleteMap)) {
                $addCondition = [$junctionField->getName() => $one_id];
                $this->deleteForeignRecords($junctionService, $junctionSchema, $junctionRefField, $deleteMap,
                    $addCondition);
            }
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to update many to one map assignment.\n{$ex->getMessage()}");
        }
    }

    /**
     * @param       $filter_info
     *
     * @return null|string
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    protected function buildQueryStringFromData($filter_info)
    {
        $filters = array_get($filter_info, 'filters');
        if (empty($filters)) {
            return null;
        }

        $sql = '';
        $combiner = array_get($filter_info, 'filter_op', DbLogicalOperators::AND_STR);
        foreach ($filters as $key => $filter) {
            if (!empty($sql)) {
                $sql .= " $combiner ";
            }

            $name = array_get($filter, 'name');
            $op = strtoupper(array_get($filter, 'operator'));
            if (empty($name) || empty($op)) {
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
            }

            if (DbComparisonOperators::requiresNoValue($op)) {
                $sql .= "($name $op)";
            } else {
                $value = array_get($filter, 'value');
                $sql .= "($name $op $value)";
            }
        }

        return $sql;
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
            $idsInfo = static::getPrimaryKeys($fields_info);
            foreach ($idsInfo as $info) {
                $requested_fields[] = $info->getName(true);
            }
        } else {
            if (false !== $requested_fields = static::validateAsArray($requested_fields, ',')) {
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
     * @param $avail_fields
     *
     * @return string
     */
    public static function getPrimaryKeyFieldFromDescribe($avail_fields)
    {
        foreach ($avail_fields as $field_info) {
            if ($field_info->isPrimaryKey) {
                return $field_info->name;
            }
        }

        return '';
    }

    /**
     * @param array   $avail_fields
     * @param boolean $names_only Return only an array of names, otherwise return all properties
     *
     * @return array
     */
    public static function getPrimaryKeys($avail_fields, $names_only = false)
    {
        $keys = [];
        foreach ($avail_fields as $info) {
            if ($info->isPrimaryKey) {
                $keys[] = ($names_only ? $info->name : $info);
            }
        }

        return $keys;
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
            if (0 >= $this->dbConn->transactionLevel()) {
                $this->dbConn->beginTransaction();
            }
        }

        $fields = array_get($extras, ApiOptions::FIELDS);
        $ssFilters = array_get($extras, 'ss_filters');
        $updates = array_get($extras, 'updates');
        $idFields = array_get($extras, 'id_fields');
        $needToIterate = ($single || !$continue || (1 < count($this->tableIdsInfo)));

        $related = array_get($extras, 'related');
        $requireMore = Scalar::boolval(array_get($extras, 'require_more')) || !empty($related);
        $allowRelatedDelete = Scalar::boolval(array_get($extras, 'allow_related_delete'));
        $relatedInfo = $this->describeTableRelated($this->transactionTable);

        $builder = $this->dbConn->table($this->transactionTable);
        if (!empty($id)) {
            if (is_array($id)) {
                foreach ($idFields as $name) {
                    $builder->where($name, array_get($id, $name));
                }
            } else {
                $name = array_get($idFields, 0);
                $builder->where($name, $id);
            }
        }

        $fields = (empty($fields)) ? $idFields : $fields;
        $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
        $bindings = array_get($result, 'bindings');
        $fields = array_get($result, 'fields');
        $fields = (empty($fields)) ? '*' : $fields;

        $serverFilter = $this->buildQueryStringFromData($ssFilters);
        if (!empty($serverFilter)) {
            Session::replaceLookups($serverFilter);
            $params = [];
            $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
            $builder->whereRaw($filterString, $params);
        }

        $out = [];
        switch ($this->getAction()) {
            case Verbs::POST:
                // need the id back in the record
                if (!empty($id)) {
                    if (is_array($id)) {
                        $record = array_merge($record, $id);
                    } else {
                        $record[array_get($idFields, 0)] = $id;
                    }
                }

                if (!empty($relatedInfo)) {
                    $this->updatePreRelations($record, $relatedInfo);
                }

                $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters);
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                    if (empty($id) && (1 === count($this->tableIdsInfo)) && $this->tableIdsInfo[0]->autoIncrement) {
                        $idName = $this->tableIdsInfo[0]->name;
                        $id[$idName] = $builder->insertGetId($parsed, $idName);
                        $record[$idName] = $id[$idName];
                    } else {
                        if (!$builder->insert($parsed)) {
                            throw new InternalServerErrorException("Record insert failed.");
                        }
                    }

                if (!empty($relatedInfo)) {
                    $this->updatePostRelations(
                        $this->transactionTable,
                        $record,
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

                if (!empty($relatedInfo)) {
                    $this->updatePreRelations($record, $relatedInfo);
                }
                $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters, true);

                // only update by ids can use batching, too complicated with ssFilters and related update
//                if ( !$needToIterate && !empty( $updates ) )
//                {
//                    return parent::addToTransaction( null, $id );
//                }

                if (!empty($parsed)) {
                    $rows = $builder->update($parsed);
                    if (0 >= $rows) {
                        // could have just not updated anything, or could be bad id
                        $result = $this->runQuery(
                            $this->transactionTable,
                            $fields,
                            $builder,
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
                    // need the id back in the record
                    if (!empty($id)) {
                        if (is_array($id)) {
                            $record = array_merge($record, $id);
                        } else {
                            $record[array_get($idFields, 0)] = $id;
                        }
                    }
                    $this->updatePostRelations(
                        $this->transactionTable,
                        $record,
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
                    $result = $this->runQuery(
                        $this->transactionTable,
                        $fields,
                        $builder,
                        $bindings,
                        $extras
                    );
                    if (empty($result)) {
                        throw new NotFoundException("Record with identifier '" . print_r($id, true) . "' not found.");
                    }

                    $out = $result[0];
                }

                $rows = $builder->delete();
                if (0 >= $rows) {
                    if (empty($out)) {
                        // could have just not updated anything, or could be bad id
                        $result = $this->runQuery(
                            $this->transactionTable,
                            $fields,
                            $builder,
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

                $result = $this->runQuery($this->transactionTable, $fields, $builder, $bindings, $extras);
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
            if (0 < $this->dbConn->transactionLevel()) {
                $this->dbConn->commit();
            }

            return null;
        }

        $updates = array_get($extras, 'updates');
        $ssFilters = array_get($extras, 'ss_filters');
        $fields = array_get($extras, ApiOptions::FIELDS);
        $idFields = array_get($extras, 'id_fields');
        $related = array_get($extras, 'related');
        $requireMore = Scalar::boolval(array_get($extras, 'require_more')) || !empty($related);
        $allowRelatedDelete = Scalar::boolval(array_get($extras, 'allow_related_delete'));
        $relatedInfo = $this->describeTableRelated($this->transactionTable);

        $builder = $this->dbConn->table($this->transactionTable);

        /** @type ColumnSchema $idName */
        $idName = (isset($this->tableIdsInfo, $this->tableIdsInfo[0])) ? $this->tableIdsInfo[0] : null;
        if (empty($idName)) {
            throw new BadRequestException('No valid identifier found for this table.');
        }

        if (!empty($this->batchRecords)) {
            if (is_array($this->batchRecords[0])) {
                $temp = [];
                foreach ($this->batchRecords as $record) {
                    $temp[] = array_get($record, $idName->getName(true));
                }

                $builder->whereIn($idName->name, $temp);
            } else {
                $builder->whereIn($idName->name, $this->batchRecords);
            }
        } else {
            $builder->whereIn($idName->name, $this->batchIds);
        }

        $serverFilter = $this->buildQueryStringFromData($ssFilters);
        if (!empty($serverFilter)) {
            Session::replaceLookups($serverFilter);
            $params = [];
            $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
            $builder->whereRaw($filterString, $params);
        }

        $out = [];
        $action = $this->getAction();
        if (!empty($this->batchRecords)) {
            if (1 == count($this->tableIdsInfo)) {
                // records are used to retrieve extras
                // ids array are now more like records
                $fields = (empty($fields)) ? $idFields : $fields;
                $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
                $bindings = array_get($result, 'bindings');
                $fields = array_get($result, 'fields');
                $fields = (empty($fields)) ? '*' : $fields;

                $result = $this->runQuery($this->transactionTable, $fields, $builder, $bindings, $extras);
                if (empty($result)) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                $out = $result;
            } else {
                $out = $this->retrieveRecords($this->transactionTable, $this->batchRecords, $extras);
            }

            $this->batchRecords = [];
        } elseif (!empty($this->batchIds)) {
            switch ($action) {
                case Verbs::PUT:
                case Verbs::MERGE:
                case Verbs::PATCH:
                    if (!empty($updates)) {
                        $parsed = $this->parseRecord($updates, $this->tableFieldsInfo, $ssFilters, true);
                        if (!empty($parsed)) {
                            $rows = $builder->update($parsed);
                            if (0 >= $rows) {
                                throw new NotFoundException('No records were found using the given identifiers.');
                            }

                            if (count($this->batchIds) !== $rows) {
                                throw new BadRequestException('Batch Error: Not all requested records could be updated.');
                            }
                        }

                        foreach ($this->batchIds as $id) {
                            if (!empty($relatedInfo)) {
                                $this->updatePostRelations(
                                    $this->transactionTable,
                                    array_merge($updates, [$idName->getName(true) => $id]),
                                    $relatedInfo,
                                    $allowRelatedDelete
                                );
                            }
                        }

                        if ($requireMore) {
                            $fields = (empty($fields)) ? $idFields : $fields;
                            $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
                            $bindings = array_get($result, 'bindings');
                            $fields = array_get($result, 'fields');
                            $fields = (empty($fields)) ? '*' : $fields;

                            $result = $this->runQuery(
                                $this->transactionTable,
                                $fields,
                                $builder,
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
                        $bindings = array_get($result, 'bindings');
                        $fields = array_get($result, 'fields');
                        $fields = (empty($fields)) ? '*' : $fields;

                        $result = $this->runQuery(
                            $this->transactionTable,
                            $fields,
                            $builder,
                            $bindings,
                            $extras
                        );
                        if (count($this->batchIds) !== count($result)) {
                            $errors = [];
                            foreach ($this->batchIds as $index => $id) {
                                $found = false;
                                if (empty($result)) {
                                    foreach ($result as $record) {
                                        if ($id == array_get($record, $idName->getName(true))) {
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

                    $rows = $builder->delete();
                    if (count($this->batchIds) !== $rows) {
                        throw new BadRequestException('Batch Error: Not all requested records were deleted.');
                    }
                    break;

                case Verbs::GET:
                    $fields = (empty($fields)) ? $idFields : $fields;
                    $result = $this->parseFieldsForSqlSelect($fields, $this->tableFieldsInfo);
                    $bindings = array_get($result, 'bindings');
                    $fields = array_get($result, 'fields');
                    $fields = (empty($fields)) ? '*' : $fields;

                    $result = $this->runQuery(
                        $this->transactionTable,
                        $fields,
                        $builder,
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
                                if ($id == array_get($record, $idName->getName(true))) {
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
                    $out[] = [$idName->getName(true) => $id];
                }
            }

            $this->batchIds = [];
        }

        if (0 < $this->dbConn->transactionLevel()) {
            $this->dbConn->commit();
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        if (0 < $this->dbConn->transactionLevel()) {
            $this->dbConn->rollBack();
        }

        return true;
    }

    public static function findRecordByNameValue($data, $field, $value)
    {
        foreach ($data as $record) {
            if (array_get($record, $field) === $value) {
                return $record;
            }
        }

        return null;
    }

    public static function getApiDocInfo(Service $service, array $resource = [])
    {
        $base = parent::getApiDocInfo($service, $resource);

//        $addTableParameters = [
//            [
//                'name'          => 'related',
//                'description'   => 'Comma-delimited list of relationship names to retrieve for each record, or \'*\' to retrieve all.',
//                'type'          => 'string',
//                'in'     => 'query',
//                'required'      => false,
//            ]
//        ];
//
//        $addTableNotes =
//            'Use the <b>related</b> parameter to return related records for each resource. ' .
//            'By default, no related records are returned.<br/> ';

        $base['definitions'] = array_merge($base['definitions'], static::getApiDocCommonModels());

        return $base;
    }
}