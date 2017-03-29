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

    public static function getDefaultPort()
    {
        return 5432;
    }

    public static function getSchema()
    {
        $schema = parent::getSchema();
        $extras = [
            [
                'name'        => 'charset',
                'label'       => 'Character Set',
                'type'        => 'string',
                'description' => 'The character set to use for this connection, i.e. ' . static::getDefaultCharset()
            ],
            [
                'name'        => 'sslmode',
                'label'       => 'SSL Mode',
                'type'        => 'string',
                'description' => 'Enable SSL mode for this connection.'
            ],
            [
                'name'        => 'timezone',
                'label'       => 'Timezone',
                'type'        => 'string',
                'description' => 'Set the timezone for this connection.'
            ],
            [
                'name'        => 'application_name',
                'label'       => 'Application Name',
                'type'        => 'string',
                'description' => 'The application used to for monitoring the application with pg_stat_activity.'
            ]
        ];

        $pos = array_search('options', array_keys($schema));
        $front = array_slice($schema, 0, $pos, true);
        $end = array_slice($schema, $pos, null, true);

        return array_merge($front, $extras, $end);
    }
}