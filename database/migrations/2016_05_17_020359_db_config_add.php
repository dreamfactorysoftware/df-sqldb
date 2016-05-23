<?php

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Migrations\Migration;

class DbConfigAdd extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        if (Schema::hasColumn('sql_db_config', 'driver')) {
            // pre-2.2 
            Schema::table('sql_db_config', function (Blueprint $t){
                $t->string('driver')->nullable()->change();
                $t->string('dsn')->nullable()->change();
                $t->text('connection')->nullable();
                $t->text('statements')->nullable();
            }
            );
        }
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        if (Schema::hasColumn('sql_db_config', 'driver')) {
            // back to pre-2.2
            Schema::table('sql_db_config', function (Blueprint $t){
                $t->string('driver')->nullable(false)->change();
                $t->string('dsn')->nullable(false)->change();
                $t->dropColumn('connection');
                $t->dropColumn('statements');
            }
            );
        }
    }
}
