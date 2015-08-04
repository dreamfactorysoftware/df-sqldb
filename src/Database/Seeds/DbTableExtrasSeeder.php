<?php
namespace DreamFactory\Core\SqlDb\Database\Seeds;

use DreamFactory\Core\Database\Seeds\BaseModelSeeder;
use DreamFactory\Core\Models\DbTableExtras;
use DreamFactory\Core\Models\Service;
use DreamFactory\Core\SqlDb\Models\SqlDbConfig;

class DbTableExtrasSeeder extends BaseModelSeeder
{
    protected $modelClass = DbTableExtras::class;

    protected $recordIdentifier = 'table';

    protected $records = [
        [
            'table' => 'sql_db_config',
            'model' => SqlDbConfig::class,
        ],
    ];

    protected function getRecordExtras()
    {
        $systemServiceId = Service::whereType('system')->pluck('id');

        return [
            'service_id' => $systemServiceId,
        ];
    }
}
