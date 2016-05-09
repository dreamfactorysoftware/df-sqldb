<?php

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Migrations\Migration;

class DbConfigAdditions extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        if (!Schema::hasColumn('sql_db_config', 'host')) {
            Schema::table(
                'sql_db_config',
                function (Blueprint $t){
                    $t->string('driver')->nullable()->change();
                    $t->string('dsn')->nullable()->change();
                    $t->string('host')->after('dsn')->nullable();
                    $t->integer('port')->after('host')->nullable();
                    $t->string('database')->after('port')->nullable();
                    $t->string('prefix')->after('password')->nullable();
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
        if (Schema::hasColumn('sql_db_config', 'host')) {
            Schema::table(
                'sql_db_config',
                function (Blueprint $t){
                    $t->string('driver')->nullable(false)->change();
                    $t->string('dsn')->nullable(false)->change();
                    $t->dropColumn('host');
                    $t->dropColumn('port');
                    $t->dropColumn('database');
                    $t->dropColumn('prefix');
                }
            );
        }
    }
}
