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

    public static function getDefaultPort()
    {
        return 3306;
    }

    public static function getDefaultConnectionInfo()
    {
        $defaults = parent::getDefaultConnectionInfo();
        $defaults[] = [
            'name'        => 'charset',
            'label'       => 'Character Set',
            'type'        => 'string',
            'description' => 'The character set to use for this connection, i.e. ' . static::getDefaultCharset()
        ];
        $defaults[] = [
            'name'        => 'collation',
            'label'       => 'Character Set Collation',
            'type'        => 'string',
            'description' => 'The character set collation to use for this connection, i.e. ' .
                static::getDefaultCollation()
        ];
        $defaults[] = [
            'name'        => 'timezone',
            'label'       => 'Timezone',
            'type'        => 'string',
            'description' => 'Set the timezone for this connection.'
        ];
        $defaults[] = [
            'name'        => 'modes',
            'label'       => 'Session Modes',
            'type'        => 'string',
            'description' => 'Connection session modes to set. Use comma-delimited string, or set \'strict\' below.'
        ];
        $defaults[] = [
            'name'        => 'strict',
            'label'       => 'Use Strict Mode',
            'type'        => 'boolean',
            'description' => 'Enable strict session mode.'
        ];
        $defaults[] = [
            'name'        => 'unix_socket',
            'label'       => 'Socket Connection',
            'type'        => 'string',
            'description' => 'The name of the socket. Do not use with host and port.'
        ];

        return $defaults;
    }
}