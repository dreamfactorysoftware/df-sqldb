<?php
namespace DreamFactory\Core\SqlDb\Models;

/**
 * PgSqlDbConfig
 *
 */
class PgSqlDbConfig extends SqlDbConfig
{
    public static function getDriverName()
    {
        return 'pgsql';
    }

    public static function getDefaultDsn()
    {
        // http://php.net/manual/en/ref.pdo-pgsql.connection.php
        return 'pgsql:host=localhost;port=5432;dbname=db;user=name;password=pwd';
    }
}