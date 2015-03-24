<?php
namespace DreamFactory\Rave\SqlDb\Database\Seeds;

use Illuminate\Database\Seeder;
use DreamFactory\Rave\Models\ServiceType;

class DatabaseSeeder extends Seeder
{
    /**
     * Run the database seeds.
     *
     * @return void
     */
    public function run()
    {
        if (!ServiceType::whereName( 'sql_db' )->exists())
        {
            // Add the service type
            ServiceType::create(
                [
                    'name'           => 'sql_db',
                    'class_name'     => 'DreamFactory\\Rave\\SqlDb\\Services\\SqlDb',
                    'config_handler' => 'DreamFactory\\Rave\\SqlDb\\Models\\SqlDbConfig',
                    'label'          => 'SQL DB',
                    'description'    => 'Database service supporting SQL connections.',
                    'group'          => 'Databases',
                    'singleton'      => false,
                ]
            );

            $this->command->info( 'SqlDb service type seeded!' );
        }
        else
        {
            $this->command->info( 'SqlDb service type already present!' );
        }
    }

}
