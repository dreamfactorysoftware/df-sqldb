<?php
namespace DreamFactory\Core\SqlDb\Models\Seeds;

use Illuminate\Database\Seeder;

class DatabaseSeeder extends Seeder
{
    public function run()
    {
        $this->call(ServiceSeeder::class);
    }
}