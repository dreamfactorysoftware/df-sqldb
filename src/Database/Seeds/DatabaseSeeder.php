<?php
namespace DreamFactory\Core\SqlDb\Database\Seeds;

use Illuminate\Database\Seeder;
use Illuminate\Database\Eloquent\Model;

class DatabaseSeeder extends Seeder
{
    /**
     * Run the database seeds.
     *
     * @return void
     */
    public function run()
    {
        Model::unguard();

        $this->call(ServiceTypeSeeder::class);
        $this->call(DbTableExtrasSeeder::class);
        $this->call(DbFieldExtrasSeeder::class);
    }
}
