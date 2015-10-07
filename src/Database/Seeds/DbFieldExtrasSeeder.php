<?php
namespace DreamFactory\Core\SqlDb\Database\Seeds;

use DreamFactory\Core\Database\Seeds\BaseModelSeeder;
use DreamFactory\Core\Models\DbFieldExtras;
use DreamFactory\Core\Models\Service;

class DbFieldExtrasSeeder extends BaseModelSeeder
{
    protected $modelClass = DbFieldExtras::class;

    protected $recordIdentifier = ['service_id','table','field'];

    protected $records = [
        [
            'field'       => 'dsn',
            'label'       => 'Connection String (DSN)',
            'description' => 'Specify the connection string for the database you\'re connecting to.',
        ],
        [
            'field'       => 'options',
            'label'       => 'Driver Options',
            'description' => 'A key=>value array of driver-specific connection options.',
        ],
        [
            'field'       => 'attributes',
            'label'       => 'Driver Attributes',
            'description' => 'A key=>value array of driver-specific attributes.',
        ],
    ];

    protected function getRecordExtras()
    {
        $systemServiceId = Service::whereType('system')->value('id');

        return [
            'service_id' => $systemServiceId,
            'table'      => 'sql_db_config',
        ];
    }
}
