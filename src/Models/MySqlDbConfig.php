<?php
namespace DreamFactory\Core\SqlDb\Models;

/**
 * MySqlDbConfig
 *
 */
class MySqlDbConfig extends SqlDbConfig
{
    public static function getDriverName()
    {
        return 'mysql';
    }

    public static function getDefaultDsn()
    {
        // http://php.net/manual/en/ref.pdo-mysql.connection.php
        return 'mysql:host=localhost;port=3306;dbname=db;charset=utf8';
    }

    public static function getDefaultPort()
    {
        return 3306;
    }
}