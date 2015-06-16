<?php
namespace DreamFactory\Core\SqlDb\Database\Seeds;

use DreamFactory\Core\Database\Seeds\BaseModelSeeder;

class DatabaseSeeder extends BaseModelSeeder
{
    protected $modelClass = 'DreamFactory\\Core\\Models\\ServiceType';

    protected $records = [
        [
            'name'           => 'sql_db',
            'class_name'     => 'DreamFactory\\Core\\SqlDb\\Services\\SqlDb',
            'config_handler' => 'DreamFactory\\Core\\SqlDb\\Models\\SqlDbConfig',
            'label'          => 'SQL DB',
            'description'    => 'Database service supporting SQL connections.',
            'group'          => 'Databases',
            'singleton'      => false,
        ]
    ];
}
