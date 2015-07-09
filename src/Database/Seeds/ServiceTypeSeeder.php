<?php
namespace DreamFactory\Core\SqlDb\Database\Seeds;

use DreamFactory\Core\Database\Seeds\BaseModelSeeder;
use DreamFactory\Core\Models\ServiceType;
use DreamFactory\Core\SqlDb\Models\SqlDbConfig;
use DreamFactory\Core\SqlDb\Services\SqlDb;

class ServiceTypeSeeder extends BaseModelSeeder
{
    protected $modelClass = ServiceType::class;

    protected $records = [
        [
            'name'           => 'sql_db',
            'class_name'     => SqlDb::class,
            'config_handler' => SqlDbConfig::class,
            'label'          => 'SQL DB',
            'description'    => 'Database service supporting SQL connections.',
            'group'          => 'Database',
            'singleton'      => false,
        ]
    ];
}
