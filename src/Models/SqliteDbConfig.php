<?php
namespace DreamFactory\Core\SqlDb\Models;

/**
 * SqliteDbConfig
 *
 */
class SqliteDbConfig extends SqlDbConfig
{
    public static function getDriverName()
    {
        return 'sqlite';
    }

    public static function getDefaultDsn()
    {
        // http://php.net/manual/en/ref.pdo-sqlite.connection.php
        return 'sqlite:db.sq3';
    }
}