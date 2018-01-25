<?php

namespace DreamFactory\Core\SqlDb\Database\Schema;

use DreamFactory\Core\Database\Components\Schema;
use DreamFactory\Core\Enums\DbResourceTypes;

/**
 * Schema is the base class for retrieving metadata information.
 *
 */
class SqlSchema extends Schema
{
    /**
     * @const string Quoting characters
     */
    const LEFT_QUOTE_CHARACTER = '"';

    /**
     * @const string Quoting characters
     */
    const RIGHT_QUOTE_CHARACTER = '"';

    /**
     * Default fetch mode for procedures and functions
     */
    const ROUTINE_FETCH_MODE = \PDO::FETCH_NAMED;

    /**
     * Return an array of supported schema resource types.
     * @return array
     */
    public function getSupportedResourceTypes()
    {
        return [
            DbResourceTypes::TYPE_SCHEMA,
            DbResourceTypes::TYPE_TABLE,
            DbResourceTypes::TYPE_TABLE_FIELD,
            DbResourceTypes::TYPE_TABLE_CONSTRAINT,
            DbResourceTypes::TYPE_TABLE_RELATIONSHIP,
            DbResourceTypes::TYPE_VIEW,
            DbResourceTypes::TYPE_FUNCTION,
            DbResourceTypes::TYPE_PROCEDURE,
        ];
    }
}
