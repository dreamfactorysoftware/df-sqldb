<?php

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Migrations\Migration;

class DefaultSchemaOnly extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        //
        Schema::table('sql_db_config', function (Blueprint $table){
            $table->boolean('default_schema_only')->default(0);
        });
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        //
        Schema::table('sql_db_config', function (Blueprint $table){
            $table->dropColumn('default_schema_only');
        });
    }
}
