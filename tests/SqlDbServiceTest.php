<?php
/**
 * This file is part of the DreamFactory Rave(tm)
 *
 * DreamFactory Rave(tm) <http://github.com/dreamfactorysoftware/rave>
 * Copyright 2012-2014 DreamFactory Software, Inc. <support@dreamfactory.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use DreamFactory\Library\Utility\Enums\Verbs;
use DreamFactory\Rave\Enums\ContentTypes;
use DreamFactory\Rave\SqlDb\Services\SqlDb;
use DreamFactory\Rave\SqlDb\Resources\Schema;
use DreamFactory\Rave\SqlDb\Resources\Table;
use DreamFactory\Rave\SqlDb\Resources\StoredProcedure;
use DreamFactory\Rave\SqlDb\Resources\StoredFunction;
use DreamFactory\Rave\Testing\TestServiceRequest;

class SqlDbServiceTest extends \DreamFactory\Rave\Testing\DbServiceTestCase
{
    /**
     * @const string
     */
    const SERVICE_NAME = 'db';
    /**
     * @const string
     */
    const TABLE_NAME = 'todo';
    /**
     * @const string
     */
    const TABLE_ID = 'id';

    /**
     * @var SqlDbService
     */
    protected $service = null;

    public function setup()
    {
        parent::setup();

        $this->service = new SqlDb(
            [
                'name'        => static::SERVICE_NAME,
                'label'       => 'SQL Database',
                'description' => 'SQL database for testing',
                'is_active'   => 1,
                'type'        => 'sql_db',
                'config'      => [ 'dsn' => env( 'SQLDB_DSN' ), 'username' => env( 'SQLDB_USER' ), 'password' => env( 'SQLDB_PASSWORD' ) ]
            ]
        );
    }

    public function tearDown()
    {
        parent::tearDown();
    }

    protected function buildPath( $path = '' )
    {
        return $this->prefix . '/' . static::SERVICE_NAME . '/' . $path;
    }

    /************************************************
     * Testing GET
     ************************************************/

    public function testDefaultResources()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest( $request );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'resource', $data );
        $this->assertCount( 4, $data['resource'] );
//        $this->assert( '_schema', $data['resource'] );
//        $this->assertCount( 3, $data['resource'] );
//        $this->assertArrayHasKey( '_proc', $data['resource'] );
//        $this->assertCount( 3, $data['resource'] );
//        $this->assertArrayHasKey( '_func', $data['resource'] );
//        $this->assertCount( 3, $data['resource'] );
    }

    public function testSchemaEmpty()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest( $request, Schema::RESOURCE_NAME );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'resource', $data );
        $this->assertEmpty( $data['resource'] );
    }

    public function testProceduresEmpty()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest( $request, StoredProcedure::RESOURCE_NAME );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'resource', $data );
        $this->assertEmpty( $data['resource'] );
    }

    public function testFunctionsEmpty()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest( $request, StoredFunction::RESOURCE_NAME );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'resource', $data );
        $this->assertEmpty( $data['resource'] );
    }

    public function testCreateTable()
    {
        $payload = '{
	"field": [
		{
			"name": "id",
			"type": "id"
		},
		{
			"name": "name",
			"type": "string",
			"is_unique": true,
			"allow_null": false
		},
		{
			"name": "complete",
			"type": "boolean",
			"allow_null": true
		}
	]
}';

        $request = new TestServiceRequest( Verbs::POST );
        $request->setContent( $payload, ContentTypes::JSON );
        $rs = $this->service->handleRequest( $request, Schema::RESOURCE_NAME . '/' . static::TABLE_NAME );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'name', $data );
        $this->assertSame( static::TABLE_NAME, $data['name'] );
    }

    public function testGetRecordsEmpty()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'record', $data );
        $this->assertEmpty( $data['record'] );
    }

    public function testCreateRecords()
    {
        $payload = '{
	"record": [
		{
			"name": "test1",
			"complete": false
		},
		{
			"name": "test2",
			"complete": true
		},
		{
			"name": "test3"
		}
	]
}';
        $request = new TestServiceRequest( Verbs::POST );
        $request->setContent( $payload, ContentTypes::JSON );
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'record', $data );
        $this->assertCount( 3, $data['record'] );
    }

    public function testGetRecordById()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1' );
        $data = $rs->getContent();
        $this->assertTrue( $data[static::TABLE_ID] == 1 );
    }

    public function testGetRecordsByIds()
    {
        $request = new TestServiceRequest( Verbs::GET, [ 'ids' => '2,3' ] );
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
        $data = $rs->getContent();
        $ids = implode( ",", array_column( $data['record'], static::TABLE_ID ) );
        $this->assertTrue($ids == "2,3");
    }

    public function testResourceNotFound()
    {
        $request = new TestServiceRequest( Verbs::GET );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/5' );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\NotFoundException', $ex );
            $this->assertEquals( 404, $ex->getCode() );
        }
    }

    /************************************************
     * Testing POST
     ************************************************/

    public function testCreateRecord()
    {
        $payload = '{"record":[{"name":"test4","complete":false}]}';
        $request = new TestServiceRequest( Verbs::POST );
        $request->setContent( $payload, ContentTypes::JSON );
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'record', $data );
        $this->assertCount( 1, $data['record'] );
    }

    public function testCreateRecordsNoWrap()
    {
        $payload = '[
		{
			"name": "test5",
			"complete": false
		},
		{
			"name": "test6",
			"complete": true
		}
	]';

        $request = new TestServiceRequest( Verbs::POST );
        $request->setContent( $payload, ContentTypes::JSON );
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'record', $data );
        $this->assertCount( 2, $data['record'] );
    }

    public function testCreateRecordReturnFields()
    {
        $payload = '{"record":[{"name":"test7","complete":true}]}';

        $request = new TestServiceRequest( Verbs::POST, [ 'fields' => 'name,complete' ] );
        $request->setContent( $payload, ContentTypes::JSON );
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'record', $data );
        $this->assertCount( 1, $data['record'] );
        $this->assertArrayHasKey( 'name', $data['record'][0] );
        $this->assertArrayHasKey( 'complete', $data['record'][0] );
    }

    public function testCreateRecordsWithContinue()
    {
        $payload = '{
	"record": [
		{
			"name": "test8",
			"complete": false
		},
		{
			"name": "test5",
			"complete": true
		},
		{
			"name": "test9",
			"complete": null
		}
	]
}';

        $request = new TestServiceRequest( Verbs::POST, [ 'continue' => true ] );
        $request->setContent( $payload, ContentTypes::JSON );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\BadRequestException', $ex );
            $this->assertEquals( 400, $ex->getCode() );
            $this->assertContains( 'Batch Error: Not all records could be created.', $ex->getMessage() );
//            $this->assertContains( "Duplicate entry 'test5'", $ex->getMessage() );
        }
    }

    public function testCreateRecordsWithRollback()
    {
        $payload = '{
	"record": [
		{
			"name": "testRollback",
			"complete": false
		},
		{
			"name": "test5",
			"complete": true
		},
		{
			"name": "testAfter"
		}
	]
}';

        $request = new TestServiceRequest( Verbs::POST, [ 'rollback' => true ] );
        $request->setContent( $payload, ContentTypes::JSON );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\InternalServerErrorException', $ex );
            $this->assertEquals( 500, $ex->getCode() );
            $this->assertContains( 'All changes rolled back.', $ex->getMessage() );
//            $this->assertContains( "Duplicate entry 'test5'", $ex->getMessage() );
        }
    }

    public function testCreateRecordBadRequest()
    {
        $payload = '{"record":[{
                        "name":"test1",
                        "complete":true
                    }]}';

        $request = new TestServiceRequest( Verbs::POST );
        $request->setContent( $payload, ContentTypes::JSON );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\InternalServerErrorException', $ex );
            $this->assertEquals( 500, $ex->getCode() );
            $this->assertContains( "Duplicate entry 'test1'", $ex->getMessage() );
        }
    }

    public function testCreateRecordFailNotNullField()
    {
        $payload = '{"record":[{
                        "name":null,
                        "complete":true
                    }]}';

        $request = new TestServiceRequest( Verbs::POST );
        $request->setContent( $payload, ContentTypes::JSON );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\BadRequestException', $ex );
            $this->assertEquals( 400, $ex->getCode() );
            $this->assertContains( "Field 'name' can not be NULL.", $ex->getMessage() );
        }
    }

    public function testCreateRecordFailMissingRequiredField()
    {
        $payload = '{"record":[{
                        "complete":true
                    }]}';

        $request = new TestServiceRequest( Verbs::POST );
        $request->setContent( $payload, ContentTypes::JSON );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\BadRequestException', $ex );
            $this->assertEquals( 400, $ex->getCode() );
            $this->assertContains( "Required field 'name' can not be NULL.", $ex->getMessage() );
        }
    }

    /************************************************
     * Testing PUT
     ************************************************/

    public function testPUTRecordById()
    {
        $this->testUpdateRecordById( Verbs::PUT );
    }

    public function testPUTRecordByIds()
    {
        $this->testUpdateRecordByIds( Verbs::PUT );
    }

    public function testPUTRecords()
    {
        $this->testUpdateRecords( Verbs::PUT );
    }

    /************************************************
     * Testing PATCH
     ************************************************/

    public function testUpdateRecordById( $verb = Verbs::PATCH )
    {
        $payload = '{"name":"test1Update"}';
        $request = new TestServiceRequest( $verb );
        $request->setContent($payload, ContentTypes::JSON);
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1' );
//        $this->assertEquals( '{"id":1}', $rs->getContent() );

        $request->setMethod( Verbs::GET );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1' );
        }
        catch ( \Exception $ex )
        {
            $this->assertTrue( false, $ex->getMessage() );
        }
    }

    public function testUpdateRecordByIds( $verb = Verbs::PATCH )
    {
//        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );
//        $this->assertEquals( "unit-test-description,unit-test-description,unit-test-description", $dColumn );
//        $this->assertEquals( "unit-test-label,unit-test-label,unit-test-label", $lColumn );
        $payload = '{"complete":true}';
        $request = new TestServiceRequest( $verb, [ 'ids' => '2,3' ] );
        $request->setContent($payload, ContentTypes::JSON);
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
//        $this->assertEquals( '{"record":[{"id":2},{"id":3}]}', $rs->getContent() );

        $request->setMethod( Verbs::GET );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
        }
        catch ( \Exception $ex )
        {
            $this->assertTrue( false, $ex->getMessage() );
        }
    }

    public function testUpdateRecords( $verb = Verbs::PATCH )
    {
//        $this->assertContains( '{"record":[{"id":1},{"id":2},{"id":3}]}', $rs->getContent() );
//        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );
//        $this->assertEquals( "unit-test-d1,unit-test-d2,unit-test-d3", $dColumn );
//        $this->assertEquals( "unit-test-l1,unit-test-l2,unit-test-l3", $lColumn );
        $payload = '{
	"record": [
		{
		    "id": 1,
			"name": "test1Update",
			"complete": false
		},
		{
		    "id": 2,
			"name": "test2Update",
			"complete": true
		},
		{
		    "id": 3,
			"name": "test3Update"
		}
	]
}';
        $request = new TestServiceRequest( $verb );
        $request->setContent( $payload, ContentTypes::JSON );
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'record', $data );
        $this->assertCount( 3, $data['record'] );
    }

    public function testUpdateRecordsWithFields( $verb = Verbs::PATCH )
    {
        $payload = '{"record":[{"id": 4, "name":"test4Update","complete":true}]}';

        $request = new TestServiceRequest( $verb, [ 'fields' => 'name,complete' ] );
        $request->setContent( $payload, ContentTypes::JSON );
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
        $data = $rs->getContent();
        $this->assertArrayHasKey( 'record', $data );
        $this->assertCount( 1, $data['record'] );
        $this->assertArrayHasKey( 'name', $data['record'][0] );
        $this->assertArrayHasKey( 'complete', $data['record'][0] );
//        $this->assertContains( '{"record":[{"label":"unit-test-l1"},{"label":"unit-test-l2"},{"label":"unit-test-l3"}]}', $rs->getContent() );
    }

    public function testUpdateRecordsWithContinue( $verb = Verbs::PATCH )
    {
        $payload = '{
	"record": [
		{
		    "id": 8,
			"name": "test8",
			"complete": false
		},
		{
		    "id": 5,
			"name": "test5",
			"complete": true
		},
		{
		    "id": 9,
			"name": "test9",
			"complete": null
		}
	]
}';

        $request = new TestServiceRequest( $verb, [ 'continue' => true ] );
        $request->setContent( $payload, ContentTypes::JSON );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\BadRequestException', $ex );
            $this->assertEquals( 400, $ex->getCode() );
            $this->assertContains( 'Batch Error: Not all records could be patched.', $ex->getMessage() );
//            $this->assertContains( "Duplicate entry 'test5'", $ex->getMessage() );
        }
//        $this->assertContains( '{"error":{"context":{"errors":[1],"record":[{"id":1},', $rs->getContent() );
//        $this->assertContains( ',{"id":3}]}', $rs->getContent() );
//        $this->assertResponseStatus( 400 );
//
//        $result = $this->call( Verbs::GET, $this->buildPath( '_table/todo?ids=1,2,3') );
//        $ra = json_decode( $result->getContent(), true );
//        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );
//
//        $this->assertEquals( "unit-test-d1,Local Database 2,unit-test-d3", $dColumn );
//        $this->assertEquals( "unit-test-l1,Database 2,unit-test-l3", $lColumn );
    }

    public function testUpdateRecordsWithRollback( $verb = Verbs::PATCH )
    {
        $payload = '{
	"record": [
		{
		    "id": 4,
			"name": "testRollback",
			"complete": false
		},
		{
		    "id": 19,
			"name": "test5",
			"complete": true
		},
		{
		    "id": 6,
			"name": "testAfter"
		}
	]
}';

        $request = new TestServiceRequest( $verb, [ 'rollback' => true ] );
        $request->setContent( $payload, ContentTypes::JSON );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\NotFoundException', $ex );
            $this->assertEquals( 404, $ex->getCode() );
            $this->assertContains( 'All changes rolled back.', $ex->getMessage() );
//            $this->assertContains( "Duplicate entry 'test5'", $ex->getMessage() );
        }
//        $this->assertContains(
//            '{"error":{"context":null,"message":"Failed to update resource: SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry \'db\' ',
//            $rs->getContent()
//        );
//        $this->assertResponseStatus( 500 );
//
//        $result = $this->call( Verbs::GET, $this->buildPath( '_table/todo?ids=1,2,3') );
//        $ra = json_decode( $result->getContent(), true );
//        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );
//
//        $this->assertEquals( "Local Database,Local Database 2,Local Database 3", $dColumn );
//        $this->assertEquals( "Database,Database 2,Database 3", $lColumn );
    }

    /************************************************
     * Testing DELETE
     ************************************************/

    public function testDeleteRecordById()
    {
        $request = new TestServiceRequest( Verbs::DELETE );
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1' );
//        $this->assertEquals( '{"id":1}', $rs->getContent() );

        $request->setMethod( Verbs::GET );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1' );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\NotFoundException', $ex );
        }
    }

    public function testDeleteRecordByIds()
    {
        $request = new TestServiceRequest( Verbs::DELETE, [ 'ids' => '2,3' ] );
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
//        $this->assertEquals( '{"record":[{"id":2},{"id":3}]}', $rs->getContent() );

        $request->setMethod( Verbs::GET );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/2' );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\NotFoundException', $ex );
        }

        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/3' );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\NotFoundException', $ex );
        }
    }

    public function testDeleteRecords()
    {
        $payload = '[{"id":4},{"id":5}]';
        $request = new TestServiceRequest( Verbs::DELETE );
        $request->setContent( $payload, ContentTypes::JSON );
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
//        $this->assertEquals( '{"record":[{"id":2},{"id":3}]}', $rs->getContent() );

        $request->setMethod( Verbs::GET );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/4' );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\NotFoundException', $ex );
        }

        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/5' );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\NotFoundException', $ex );
        }
    }

    public function testDeleteRecordsWithFields()
    {
        $payload = '[{"id":6},{"id":7}]';
        $request = new TestServiceRequest( Verbs::DELETE, [ 'fields' => 'name' ] );
        $request->setContent( $payload, ContentTypes::JSON );
        $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME );
//        $this->assertEquals( '{"record":[{"id":2},{"id":3}]}', $rs->getContent() );

        $request->setMethod( Verbs::GET );
        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/6' );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\NotFoundException', $ex );
        }

        try
        {
            $rs = $this->service->handleRequest( $request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/7' );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\NotFoundException', $ex );
        }
    }

    public function testDropTable()
    {
        $request = new TestServiceRequest( Verbs::DELETE );
        $rs = $this->service->handleRequest( $request, Schema::RESOURCE_NAME . '/' . static::TABLE_NAME );

        $request->setMethod( Verbs::GET );
        try
        {
            $rs = $this->service->handleRequest( $request, Schema::RESOURCE_NAME . '/' . static::TABLE_NAME );
            $this->assertTrue( false );
        }
        catch ( \Exception $ex )
        {
            $this->assertInstanceOf( '\DreamFactory\Rave\Exceptions\NotFoundException', $ex );
        }
    }
}
