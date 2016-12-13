<?php
namespace DreamFactory\Core\SqlDb\Models\Seeds;

use DreamFactory\Core\Models\Seeds\BaseModelSeeder;
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
            'type'        => 'sqlite',
            'mutable'     => true,
            'deletable'   => true,
            'config'      => ['database' => 'db.sqlite']
        ]
    ];
}
