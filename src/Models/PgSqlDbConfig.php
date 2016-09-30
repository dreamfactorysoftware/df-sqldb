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

    protected function getConnectionFields()
    {
        $fields = parent::getConnectionFields();

        return array_merge($fields, ['charset', 'sslmode', 'timezone', 'application_name']);
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
            'name'        => 'sslmode',
            'label'       => 'SSL Mode',
            'type'        => 'string',
            'description' => 'Enable SSL mode for this connection.'
        ];
        $defaults[] = [
            'name'        => 'timezone',
            'label'       => 'Timezone',
            'type'        => 'string',
            'description' => 'Set the timezone for this connection.'
        ];
        $defaults[] = [
            'name'        => 'application_name',
            'label'       => 'Application Name',
            'type'        => 'string',
            'description' => 'The application used to for monitoring the application with pg_stat_activity.'
        ];

        return $defaults;
    }
}