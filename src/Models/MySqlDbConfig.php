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

    public static function getSchema()
    {
        $schema = parent::getSchema();
        $extras = [
            'charset' => [
                'name'        => 'charset',
                'label'       => 'Character Set',
                'type'        => 'string',
                'description' => 'The character set to use for this connection, i.e. ' . static::getDefaultCharset()
            ],
            'collation' => [
                'name'        => 'collation',
                'label'       => 'Character Set Collation',
                'type'        => 'string',
                'description' => 'The character set collation to use for this connection, i.e. ' .
                    static::getDefaultCollation()
            ],
            'timezone' => [
                'name'        => 'timezone',
                'label'       => 'Timezone',
                'type'        => 'string',
                'description' => 'Set the timezone for this connection.'
            ],
            'modes' => [
                'name'        => 'modes',
                'label'       => 'Session Modes',
                'type'        => 'string',
                'description' => 'Connection session modes to set. Use comma-delimited string, or set \'strict\' below.'
            ],
            'strict' => [
                'name'        => 'strict',
                'label'       => 'Use Strict Mode',
                'type'        => 'boolean',
                'description' => 'Enable strict session mode.'
            ],
            'unix_socket' => [
                'name'        => 'unix_socket',
                'label'       => 'Socket Connection',
                'type'        => 'string',
                'description' => 'The name of the socket. Do not use with host and port.'
            ]
        ];

        $pos = array_search('options', array_keys($schema));
        $front = array_slice($schema, 0, $pos, true);
        $end = array_slice($schema, $pos, null, true);

        return array_merge($front, $extras, $end);
    }
}