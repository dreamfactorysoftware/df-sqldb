<?php
namespace DreamFactory\Core\SqlDb\Database\Seeds;

use DreamFactory\Core\Database\Seeds\BaseModelSeeder;
use DreamFactory\Core\Models\Service;

class ServiceSeeder extends BaseModelSeeder
{
    protected $modelClass = Service::class;

    protected $records = [
        [
            'name'        => 'db',
            'label'       => 'Local SQL Database',
            'description' => 'Service for accessing local SQLite database.',
            'is_active'   => true,
            'type'        => 'sql_db',
            'mutable'     => true,
            'deletable'   => true,
            'config' => [
                'driver' => 'sqlite',
                'dsn' => 'sqlite:db.sqlite'
            ]
        ]
    ];
}
