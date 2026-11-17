<?php

namespace DreamFactory\Core\SqlDb\Resources;

use DB;
use DreamFactory\Core\Database\Components\Expression;
use DreamFactory\Core\Database\Components\RelationTransactionContext;
use DreamFactory\Core\Database\Enums\DbFunctionUses;
use DreamFactory\Core\Database\Resources\BaseDbTableResource;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\RelationSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\BatchException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Core\Enums\Verbs;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Collection;

/**
 * Class Table
 *
 * @package DreamFactory\Core\SqlDb\Resources
 */
class Table extends BaseDbTableResource
{
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
     * {@inheritdoc}
     */
    public function updateRecordsByFilter($table, $record, $filter = null, $params = [], $extras = [])
    {
        $record = static::validateAsArray($record, null, false, 'There are no fields in the record.');

        $idFields = array_get($extras, ApiOptions::ID_FIELD);
        $idTypes = array_get($extras, ApiOptions::ID_TYPE);
        $related = array_get($extras, ApiOptions::RELATED);
        $allowRelatedDelete = array_get_bool($extras, ApiOptions::ALLOW_RELATED_DELETE);
        $ssFilters = array_get($extras, 'ss_filters');

        try {
            if (!$tableSchema = $this->parent->getTableSchema($table)) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }

            $fieldsInfo = $tableSchema->getColumns(true);
            $idsInfo = $this->getIdsInfo($table, $fieldsInfo, $idFields, $idTypes);
            $relatedInfo = $tableSchema->getRelations(true);
            $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters, true);

            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->parent->getConnection()->table($tableSchema->internalName);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            if (!empty($parsed)) {
                $builder->update($parsed);
            }

            $results = $this->runQuery($table, $builder, $extras);

            if (!empty($relatedInfo)) {
                // update related info
                foreach ($results as $row) {
                    $this->checkForIds($row, $idsInfo, $extras);
                    $this->updatePostRelations($table, array_merge($row, $record), $relatedInfo, $allowRelatedDelete);
                }
                // get latest with related changes if requested
                if (!empty($related)) {
                    $results = $this->runQuery($table, $builder, $extras);
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
            if (!$tableSchema = $this->parent->getTableSchema($table)) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }
            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->parent->getConnection()->table($tableSchema->internalName);
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
        $ssFilters = array_get($extras, 'ss_filters');

        try {
            if (!$tableSchema = $this->parent->getTableSchema($table)) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }
            $fieldsInfo = $tableSchema->getColumns(true);
            /*$idsInfo = */
            $this->getIdsInfo($table, $fieldsInfo, $idFields, $idTypes);

            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->parent->getConnection()->table($tableSchema->internalName);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            $results = $this->runQuery($table, $builder, $extras);

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
        $ssFilters = array_get($extras, 'ss_filters');

        try {
            $tableSchema = $this->parent->getTableSchema($table);
            if (!$tableSchema) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }

            $fieldsInfo = $tableSchema->getColumns(true);

            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->parent->getConnection()->table($tableSchema->internalName);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            return $this->runQuery($table, $builder, $extras);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to retrieve records from '$table'.\n{$ex->getMessage()}");
        }
    }

    // Helper methods

    protected function restrictFieldsToDefined()
    {
        return true;
    }

    /**
     * @param              $table
     * @param Builder      $builder
     * @param array        $extras
     * @return int|array
     * @throws BadRequestException
     * @throws InternalServerErrorException
     * @throws NotFoundException
     * @throws RestException
     */
    protected function runQuery($table, Builder $builder, $extras)
    {
        $schema = $this->parent->getTableSchema($table);
        if (!$schema) {
            throw new NotFoundException("Table '$table' does not exist in the database.");
        }

        $limit = intval(array_get($extras, ApiOptions::LIMIT, 0));
        $offset = intval(array_get($extras, ApiOptions::OFFSET, 0));
        $countOnly = array_get_bool($extras, ApiOptions::COUNT_ONLY);
        $includeCount = array_get_bool($extras, ApiOptions::INCLUDE_COUNT);

        $maxAllowed = $this->getMaxRecordsReturnedLimit();
        $needLimit = false;
        if (($limit < 1) || ($limit > $maxAllowed)) {
            // impose a limit to protect server
            $limit = $maxAllowed;
            $needLimit = true;
        }

        // count total records
        $count = 0;
        if ($countOnly || $includeCount || $needLimit) {
            $count = $builder->count([DB::raw('1')]);
        }

        if ($countOnly) {
            return $count;
        }

        // apply the selected fields
        $select = $this->parseSelect($schema, $extras);
        $builder->select($select);

        // apply the rest of the parameters
        $order = trim(array_get($extras, ApiOptions::ORDER));
        if (!empty($order)) {
            if (false !== strpos($order, ';')) {
                throw new BadRequestException('Invalid order by clause in request.');
            }

            $orderComponents = explode(',', $order);
            foreach ($orderComponents as $component) {
                $component = trim($component);
                $parts = explode(' ', $component);

                if (count($parts) < 1) {
                    continue;
                }

                $field = $parts[0];

                // Validate field against schema columns to prevent SQL injection
                if (!$schema->getColumn($field, true)) {
                    throw new BadRequestException("Invalid field '$field' in order by clause.");
                }

                $direction = 'asc';
                if (isset($parts[1]) && in_array(strtolower($parts[1]), ['asc', 'desc'])) {
                    $direction = strtolower($parts[1]);
                }

                $col = $schema->getColumn($field, true);

                $nullsOrdering = '';
                if (isset($parts[2]) && strtolower($parts[2]) === 'nulls' && isset($parts[3]) && in_array(strtolower($parts[3]), ['first', 'last'])) {
                    $nullsOrdering = ' NULLS ' . strtoupper($parts[3]);
                }

                if (!empty($nullsOrdering)) {
                    // orderByRaw needs pre-quoted names since it's raw SQL
                    $builder->orderByRaw($col->quotedName . ' ' . $direction . $nullsOrdering);
                } else {
                    // orderBy() quotes internally, so use the plain name
                    $builder->orderBy($col->name, $direction);
                }
            }
        }
        $group = trim(array_get($extras, ApiOptions::GROUP));
        if (!empty($group)) {
            $group = static::fieldsToArray($group);
            $groups = $this->parseGroupBy($schema, $group);
            $builder->groupBy($groups);
        }
        $builder->take($limit);
        $builder->skip($offset);

        $result = $this->getQueryResults($schema, $builder, $extras);

        if (!empty($result)) {
            $related = array_get($extras, ApiOptions::RELATED);
            if (!empty($related) || $schema->fetchRequiresRelations) {
                if (ApiOptions::FIELDS_ALL !== $related) {
                    $related = static::fieldsToArray($related);
                }
                $refresh = array_get_bool($extras, ApiOptions::REFRESH);
                /** @type RelationSchema[] $availableRelations */
                $availableRelations = $schema->getRelations(true);
                if (!empty($availableRelations)) {
                    // until this is refactored to collections
                    $data = $result->toArray();
                    $this->retrieveRelatedRecords($schema, $availableRelations, $related, $data, $refresh);
                    $result = collect($data);
                }
            }
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

        if (array_get_bool($extras, ApiOptions::INCLUDE_SCHEMA)) {
            try {
                $meta['schema'] = $schema->toArray(true);
            } catch (RestException $ex) {
                throw $ex;
            } catch (\Exception $ex) {
                throw new InternalServerErrorException("Error describing database table '$table'.\n" .
                    $ex->getMessage(), $ex->getCode());
            }
        }

        $data = $result->toArray();
        if (!empty($meta)) {
            $data['meta'] = $meta;
        }

        return $data;
    }

    /**
     * @param TableSchema $schema
     * @param Builder     $builder
     * @param array       $extras
     * @return Collection
     */
    protected function getQueryResults(TableSchema $schema, Builder $builder, $extras)
    {
        $result = $builder->get();

        $result->transform(function ($item) use ($schema) {
            $item = (array)$item;
            foreach ($item as $field => &$value) {
                if ($fieldInfo = $schema->getColumn($field, true)) {
                    $value = $this->parent->getSchema()->typecastToClient($value, $fieldInfo);
                }
            }

            return $item;
        });

        return $result;
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
    ) {
        // interpret any parameter values as lookups
        $params = (is_array($params) ? static::interpretRecordValues($params) : []);
        $serverFilter = $this->buildQueryStringFromData($ss_filters);

        // Apply the client filter and the role (server-side) filter as two
        // separate builder groups, never as one concatenated string. The query
        // builder parenthesizes each group on its own, so neither filter can
        // alter the other's structure, and every value is bound as a parameter
        // so string role filters keep their quoting. Each group's emitted SQL
        // must also have balanced parentheses (see applyFilterGroup).
        $clientFilter = is_string($filter) ? $filter : '';

        if ('' !== trim($clientFilter)) {
            $this->applyFilterGroup($builder, $clientFilter, $params, $avail_fields);
        }

        if (!empty($serverFilter)) {
            $this->applyFilterGroup($builder, $serverFilter, $params, $avail_fields);
        }
    }

    /**
     * Parse one filter string and apply it to the builder as its own nested WHERE
     * group. The closure makes the query builder wrap the group in parentheses, so
     * the group's internal structure (its ANDs and ORs) cannot merge with any other
     * group's. Values are bound as parameters by parseFilterString(), which keeps
     * string values correctly quoted.
     *
     * @param \Illuminate\Database\Query\Builder $builder
     * @param string                             $filter
     * @param array                              $in_params
     * @param array                              $avail_fields
     *
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function applyFilterGroup(Builder $builder, $filter, array $in_params, $avail_fields)
    {
        Session::replaceLookups($filter);
        $outParams = [];
        $filterString = $this->parseFilterString($filter, $outParams, $avail_fields, $in_params);
        if (!empty($filterString)) {
            // The parser splits on logical operators without regard to quotes and
            // passes stray parentheses through. So the check runs on what the
            // parser emits, not on the input: a fragment whose parentheses do not
            // balance would pair with the builder's wrapping parentheses and
            // change the structure of the WHERE clause.
            if (!static::parenthesesAreBalanced($filterString)) {
                throw new BadRequestException('Unbalanced parentheses in filter request.');
            }
            $builder->where(function ($query) use ($filterString, $outParams) {
                $query->whereRaw($filterString, $outParams);
            });
        }
    }

    /**
     * True when every parenthesis in an emitted SQL fragment is matched and no
     * prefix closes a group it never opened. Quotes are deliberately ignored:
     * values are bound as parameters, so the fragment carries no user string
     * literals, and a quote-aware scan of the input can disagree with the parser.
     *
     * @param string $sql
     *
     * @return bool
     */
    protected static function parenthesesAreBalanced($sql)
    {
        $depth = 0;
        $len = strlen($sql);
        for ($i = 0; $i < $len; $i++) {
            if ($sql[$i] === '(') {
                $depth++;
            } elseif ($sql[$i] === ')' && --$depth < 0) {
                return false;
            }
        }

        return $depth === 0;
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

        // Accept a='x' OR b='y' as well as (a='x') OR (b='y') (df-core#172).
        $filter = DbLogicalOperators::wrapBareConditions(trim($filter));
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
            // (a=1 AND b=2): the wrapper hid a bare logical operator from
            // wrapBareConditions() above (it only splits at depth 0), so the
            // parser would bind "1 AND b=2" as the value. Re-run on the inner.
            $inner = DbLogicalOperators::wrapBareConditions($filter);
            if ($inner !== $filter) {
                return '(' . $this->parseFilterString($inner, $out_params, $fields_info, $in_params) . ')';
            }
        }

        // Splitting on logical operators can leave grouping parentheses at either
        // end of a condition. Set aside only the leading run of "(" and the
        // trailing run of ")". A function call in the value, such as NOW() in
        // "born < NOW()", keeps its own "(" here; its ")" is handed back to the
        // value below.
        [$leftParen, $filter, $rightParen] = static::splitOuterParentheses($filter);

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
                    ($lpc > $rpc = substr_count($value, ')'))
                ) {
                    // add back to value from right
                    $value .= str_repeat(')', $lpc - $rpc);
                    $rightParen = substr((string)$rightParen, $lpc - $rpc);
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

                if ($function = $info->getDbFunction(DbFunctionUses::FILTER)) {
                    // Plain string, not a Query\Expression: $out is
                    // string-concatenated into the WHERE clause immediately
                    // below, and Expression stopped being stringable in
                    // Laravel 10 - wrapping it in raw() fatals with "Object of
                    // class Illuminate\Database\Query\Expression could not be
                    // converted to string". The SELECT path (buildQueryFields)
                    // still uses raw(), correctly: there the value is handed to
                    // the query builder as an Expression rather than concatenated.
                    $out = $function;
                } else {
                    $out = $info->quotedName;
                }
                $out .= " $sqlOp";
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
            } elseif (static::isSafeFilterExpression($value)) {
                // One of the recognized no-argument SQL functions, written as
                // NOW() or (NOW()), is used as is. Any other value, including
                // other parenthesized text, is bound as a parameter below.
                return $value;
            }
        }

        // anything else schema specific
        $value = $this->parent->getSchema()->typecastToNative($value, $info);

        $out_params[] = $value;
        $value = '?';

        return $value;
    }

    /**
     * @inheritdoc
     */
    protected function getCurrentTimestamp()
    {
        return $this->parent->getSchema()->getTimestampForSet();
    }

    /**
     * @inheritdoc
     */
    protected function parseValueForSet($value, $field_info, $for_update = false)
    {
        if ($value instanceof Expression) {
            $expr = trim($value->expression);
            if (strtoupper($expr) === 'NULL') {
                return DB::raw('NULL');
            }
            if (!static::isSafeExpression($expr)) {
                throw new BadRequestException("SQL expression '$expr' is not permitted.");
            }
            return DB::raw($expr);
        } else {
            return parent::parseValueForSet($value, $field_info, $for_update);
        }
    }

    /**
     * True when a filter value is one of the recognized no-argument SQL functions
     * that can be used directly, either bare (NOW()) or in one pair of
     * parentheses ((NOW())). Everything else is bound as a parameter.
     */
    protected static function isSafeFilterExpression(string $value): bool
    {
        // Recognize a small set of well-known no-argument SQL functions that may be
        // passed through as-is. Any other value is treated as a literal and bound as
        // a parameter by parseFilterValue().
        $function = '(?:NOW|CURDATE|CURTIME|UUID|CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME|GETDATE|GETUTCDATE|NEWID|SYSDATE|SYSDATETIME)\(\)';

        return (bool) preg_match('/^(?:' . $function . '|\(' . $function . '\))$/i', trim($value));
    }

    /**
     * Split a single condition into the grouping parentheses around it and the
     * condition itself: [leading "(" run or null, condition, trailing ")" run or
     * null]. Only "(" are taken from the start and only ")" from the end, so
     * "born < NOW())" gives [null, "born < NOW(", "))"]; parseFilterString()
     * then returns the ")" the value needs.
     *
     * @param string $condition
     *
     * @return array
     */
    protected static function splitOuterParentheses($condition)
    {
        $condition = (string)$condition;
        $leading = strspn($condition, '(');
        $body = substr($condition, $leading);
        $inner = rtrim($body, ')');
        $trailing = strlen($body) - strlen($inner);

        return [
            $leading ? substr($condition, 0, $leading) : null,
            $inner,
            $trailing ? substr($body, -$trailing) : null,
        ];
    }

    /**
     * True when an {"expression": "..."} value is one of the recognized no-argument
     * SQL functions (or NULL) that can be used directly. Other values go through
     * normal value binding.
     */
    protected static function isSafeExpression(string $expr): bool
    {
        // Recognize the supported no-argument SQL functions (and NULL) for the
        // expression form. Other values are handled through normal value binding.
        return (bool) preg_match(
            '/^(NOW|CURDATE|CURTIME|UUID|CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME|GETDATE|GETUTCDATE|NEWID|SYSDATE|SYSDATETIME|NULL)(\(\))?$/i',
            trim($expr)
        );
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
     * @param ColumnSchema $field
     *
     * @return \Illuminate\Database\Query\Expression|string
     */
    protected function parseFieldForSelect($field)
    {
        if ($function = $field->getDbFunction(DbFunctionUses::SELECT)) {
            return $this->parent->getConnection()->raw($function . ' AS ' . $field->getName(true, true));
        }

        $out = $field->name;
        if (!empty($field->alias)) {
            $out .= ' AS ' . $field->alias;
        }

        return $out;
    }

    protected static function fieldsToArray($fields)
    {
        if (empty($fields) || (ApiOptions::FIELDS_ALL === $fields)) {
            return [];
        }

        return (!is_array($fields)) ? array_map('trim', explode(',', trim($fields, ','))) : $fields;
    }

    /**
     * @param  TableSchema $schema
     * @param  array|null  $extras
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseSelect($schema, $extras)
    {
        $fields = array_get($extras, ApiOptions::FIELDS);
        if (empty($fields)) {
            // minimally return the id fields
            $fields = array_get($extras, ApiOptions::ID_FIELD);
            if (empty($fields)) {
                $fields = $schema->getPrimaryKey();
                // if still nothing, return everything
                if (empty($fields)) {
                    $fields = ApiOptions::FIELDS_ALL;
                }
            }
        }
        $outArray = [];
        if (ApiOptions::FIELDS_ALL === $fields) {
            foreach ($schema->getColumns() as $fieldInfo) {
                if ($fieldInfo->isAggregate) {
                    continue;
                }
                $out = $this->parseFieldForSelect($fieldInfo);
                if (is_array($out)) {
                    $outArray = array_merge($outArray, $out);
                } else {
                    $outArray[] = $out;
                }
            }
        } else {
            $fields = static::fieldsToArray($fields);
            $related = array_get($extras, ApiOptions::RELATED);
            $allRelated = ('*' === $related);
            $related = static::fieldsToArray($related);
            if ($allRelated || !empty($related) || $schema->fetchRequiresRelations) {
                // add any required relationship mapping fields
                foreach ($schema->getRelations() as $relation) {
                    if ($relation->alwaysFetch || $allRelated || array_key_exists($relation->getName(true), $related)) {
                        foreach ($relation->field as $relField) {
                            if ($fieldInfo = $schema->getColumn($relField)) {
                                $relationField = $fieldInfo->getName(true); // account for aliasing
                                if (false === array_search($relationField, $fields)) {
                                    $fields[] = $relationField;
                                }
                            }
                        }
                    }
                }
            }
            foreach ($fields as $field) {
                if ($fieldInfo = $schema->getColumn($field, true)) {
                    $out = $this->parseFieldForSelect($fieldInfo);
                    if (is_array($out)) {
                        $outArray = array_merge($outArray, $out);
                    } else {
                        $outArray[] = $out;
                    }
                } elseif ($aggExpr = $this->parseAggregateExpression($schema, $field)) {
                    // Aggregates are valid with GROUP BY, or alone (whole-set MIN/MAX/COUNT/...).
                    // Mixing plain columns with aggregates and no GROUP BY is left to the database to reject.
                    $outArray[] = $aggExpr;
                } else {
                    throw new BadRequestException('Invalid field requested: ' . $field);
                }
            }
        }

        return empty($outArray) ? ['*'] : $outArray;
    }

    /**
     * @param  TableSchema $schema
     * @param  array       $fields
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseGroupBy($schema, $fields = null)
    {
        $outArray = [];
        if (!empty($fields)) {
            foreach ($fields as $field) {
                if ($fieldInfo = $schema->getColumn($field, true)) {
                    $outArray[] = $fieldInfo->name;
                } else {
                    throw new BadRequestException("Invalid or unknown field '$field' in group by clause.");
                }
            }
        }

        return $outArray;
    }

    /**
     * Parse an ad-hoc aggregate expression like SUM(column_name) or COUNT(*).
     * Allowed with or without GROUP BY. Validates that the inner column
     * exists in the table schema to prevent SQL injection.
     *
     * @param  TableSchema $schema
     * @param  string      $field
     *
     * @return \Illuminate\Database\Query\Expression|null
     */
    protected function parseAggregateExpression($schema, $field)
    {
        $allowed = ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX'];
        $pattern = '/^(' . implode('|', $allowed) . ')\s*\(\s*(.+?)\s*\)$/i';

        if (!preg_match($pattern, trim($field), $matches)) {
            return null;
        }

        $func = strtoupper($matches[1]);
        $inner = $matches[2];

        // COUNT(*) is always valid
        if ($func === 'COUNT' && $inner === '*') {
            return DB::raw('COUNT(*) AS COUNT_ALL');
        }

        // Reject anything that looks like sub-expressions or injection
        if (preg_match('/[;\'"\(\)\\\\]/', $inner)) {
            return null;
        }

        // Validate the inner column exists in the schema
        $columnInfo = $schema->getColumn($inner, true);
        if (!$columnInfo) {
            return null;
        }

        $columnName = $columnInfo->name;
        $alias = $func . '_' . preg_replace('/[^a-zA-Z0-9_]/', '_', $columnInfo->getName(true));

        return DB::raw($func . '(' . $this->parent->getConnection()->getQueryGrammar()->wrap($columnName) . ') AS ' . $alias);
    }

    /**
     * @param      $table
     * @param null $fields_info
     * @param null $requested_fields
     * @param null $requested_types
     *
     * @return array|\DreamFactory\Core\Database\Schema\ColumnSchema[]
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
    ) {
        $dbConn = $this->parent->getConnection();
        if ($rollback) {
            // sql transaction really only for rollback scenario, not batching
            if (0 >= $dbConn->transactionLevel()) {
                $dbConn->beginTransaction();
            }
        }

        if (!isset($extras['limit'])) {
            $extras['limit'] = 1;
        }
        $ssFilters = array_get($extras, 'ss_filters');
        $updates = array_get($extras, 'updates');
        $idFields = array_get($extras, 'id_fields');
        $needToIterate = ($single || !$continue || (1 < count($this->tableIdsInfo)));

        $related = array_get($extras, 'related');
        $requireMore = array_get_bool($extras, 'require_more') || !empty($related);
        $allowRelatedDelete = array_get_bool($extras, 'allow_related_delete');
        $relatedInfo = $this->describeTableRelated($this->transactionTable);

        // Create transaction context for related record operations
        $context = ($rollback || $continue) ? new RelationTransactionContext($rollback, $continue) : null;

        $builder = $dbConn->table($this->transactionTableSchema->internalName);
        $match = [];
        if (!is_null($id)) {
            if (is_array($id)) {
                $match = $id;
                foreach ($idFields as $name) {
                    $builder->where($name, array_get($id, $name));
                }
            } else {
                $name = array_get($idFields, 0);
                $match[$name] = $id;
                $builder->where($name, $id);
            }
        }

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
                if (!empty($match)) {
                    $record = array_merge($record, $match);
                }

                if (!empty($relatedInfo)) {
                    $this->updatePreRelations($record, $relatedInfo, $context);
                }

                $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters);
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                if (is_null($id) && (1 === count($this->tableIdsInfo)) && $this->tableIdsInfo[0]->autoIncrement) {
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
                        $allowRelatedDelete,
                        $context
                    );

                    // Handle relation errors with rollback
                    if ($context && $context->hasErrors() && $context->shouldRollback()) {
                        $context->executeCompensations([$this, 'handleVirtualRecords']);
                        throw new BadRequestException(
                            "Related record operations failed. All changes rolled back. Errors: " .
                            json_encode($context->getErrorMessages())
                        );
                    }
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
            case Verbs::PATCH:
                if (!empty($updates)) {
                    $record = $updates;
                }

                if (!empty($relatedInfo)) {
                    $this->updatePreRelations($record, $relatedInfo, $context);
                }

                $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters, true);
                if (!empty($parsed)) {
                    if (!empty($match) && $this->parent->upsertAllowed() && !$builder->exists()) {
                        if (!$builder->insert(array_merge($match, $parsed))) {
                            throw new InternalServerErrorException("Record upsert failed.");
                        }
                    } else {
                        $parsed = array_except($parsed, array_keys($match));
                        $rows = $builder->update($parsed);
                        if (0 >= $rows) {
                            // could have just not updated anything, or could be bad id
                            if (empty($this->runQuery($this->transactionTable, $builder, $extras))) {
                                throw new NotFoundException("Record with identifier '" .
                                    print_r($id, true) .
                                    "' not found.");
                            }
                        }
                    }
                }

                if (!empty($relatedInfo)) {
                    // need the id back in the record
                    if (!empty($match)) {
                        $record = array_merge($record, $match);
                    }
                    $this->updatePostRelations(
                        $this->transactionTable,
                        $record,
                        $relatedInfo,
                        $allowRelatedDelete,
                        $context
                    );

                    // Handle relation errors with rollback
                    if ($context && $context->hasErrors() && $context->shouldRollback()) {
                        $context->executeCompensations([$this, 'handleVirtualRecords']);
                        throw new BadRequestException(
                            "Related record operations failed. All changes rolled back. Errors: " .
                            json_encode($context->getErrorMessages())
                        );
                    }
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
                        $builder,
                        $extras
                    );
                    if (empty($result)) {
                        // bail, we know it isn't there
                        throw new NotFoundException("Record with identifier '" . print_r($id, true) . "' not found.");
                    }

                    $out = $result[0];
                }

                if (1 > $builder->delete()) {
                    // wasn't anything there to delete
                    throw new NotFoundException("Record with identifier '" . print_r($id, true) . "' not found.");
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

                $result = $this->runQuery($this->transactionTable, $builder, $extras);
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
        $dbConn = $this->parent->getConnection();
        if (empty($this->batchRecords) && empty($this->batchIds)) {
            if (0 < $dbConn->transactionLevel()) {
                $dbConn->commit();
            }

            return null;
        }

        $updates = array_get($extras, 'updates');
        $ssFilters = array_get($extras, 'ss_filters');
        $related = array_get($extras, 'related');
        $requireMore = array_get_bool($extras, 'require_more') || !empty($related);
        $allowRelatedDelete = array_get_bool($extras, 'allow_related_delete');
        $relatedInfo = $this->describeTableRelated($this->transactionTable);

        // Create context for batch commit post-relations
        $rollback = array_get_bool($extras, ApiOptions::ROLLBACK, false);
        $continue = array_get_bool($extras, ApiOptions::CONTINUES, false);
        $context = ($rollback || $continue) ? new RelationTransactionContext($rollback, $continue) : null;

        $builder = $dbConn->table($this->transactionTableSchema->internalName);

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
                $result = $this->runQuery($this->transactionTable, $builder, $extras);
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
                case Verbs::PATCH:
                    if (!empty($updates)) {
                        $parsed = $this->parseRecord($updates, $this->tableFieldsInfo, $ssFilters, true);
                        if (!empty($parsed)) {
                            $rows = $builder->update($parsed);
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
                                    $allowRelatedDelete,
                                    $context
                                );
                            }
                        }

                        if ($requireMore) {
                            $result = $this->runQuery(
                                $this->transactionTable,
                                $builder,
                                $extras
                            );

                            $out = $result;
                        }
                    }
                    break;

                case Verbs::DELETE:
                    $result = $this->runQuery(
                        $this->transactionTable,
                        $builder,
                        $extras
                    );
                    if (count($this->batchIds) !== count($result)) {
                        foreach ($this->batchIds as $index => $id) {
                            $found = false;
                            foreach ($result as $record) {
                                if ($id == array_get($record, $idName->getName(true))) {
                                    $out[$index] = $record;
                                    $found = true;
                                    break;
                                }
                            }
                            if (!$found) {
                                $out[$index] = new NotFoundException("Record with identifier '" . print_r($id,
                                        true) . "' not found.");
                            }
                        }
                    } else {
                        $out = $result;
                    }

                    $rows = $builder->delete();
                    if (count($this->batchIds) !== $rows) {
                        throw new BatchException($out, 'Batch Error: Not all requested records could be deleted.');
                    }
                    break;

                case Verbs::GET:
                    $result = $this->runQuery(
                        $this->transactionTable,
                        $builder,
                        $extras
                    );

                    if (count($this->batchIds) !== count($result)) {
                        foreach ($this->batchIds as $index => $id) {
                            $found = false;
                            foreach ($result as $record) {
                                if ($id == array_get($record, $idName->getName(true))) {
                                    $out[$index] = $record;
                                    $found = true;
                                    break;
                                }
                            }
                            if (!$found) {
                                $out[$index] = new NotFoundException("Record with identifier '" . print_r($id,
                                        true) . "' not found.");
                            }
                        }

                        throw new BatchException($out, 'Batch Error: Not all requested records could be retrieved.');
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

        if (0 < $dbConn->transactionLevel()) {
            $dbConn->commit();
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        $dbConn = $this->parent->getConnection();
        if (0 < $dbConn->transactionLevel()) {
            $dbConn->rollBack();
        }

        return true;
    }
}