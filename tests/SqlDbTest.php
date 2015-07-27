<?php

use DreamFactory\Library\Utility\Enums\Verbs;
use DreamFactory\Core\Enums\DataFormats;
use DreamFactory\Core\SqlDb\Services\SqlDb;
use DreamFactory\Core\SqlDb\Resources\Schema;
use DreamFactory\Core\SqlDb\Resources\Table;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;
use DreamFactory\Core\SqlDb\Resources\StoredFunction;
use DreamFactory\Core\Testing\TestServiceRequest;
use DreamFactory\Core\Enums\ApiOptions;

class SqlDbTest extends \DreamFactory\Core\Testing\DbServiceTestCase
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
     * @var SqlDb
     */
    protected $service = null;

    public function setup()
    {
        parent::setup();

        $this->service = new SqlDb(
            [
                'id'          => 1,
                'name'        => static::SERVICE_NAME,
                'label'       => 'SQL Database',
                'description' => 'SQL database for testing',
                'is_active'   => true,
                'type'        => 'sql_db',
                'config'      => ['dsn'      => env('SQLDB_DSN'),
                                  'username' => env('SQLDB_USER'),
                                  'password' => env('SQLDB_PASSWORD')
                ]
            ]
        );
    }

    public function tearDown()
    {
        parent::tearDown();
    }

    protected function buildPath($path = '')
    {
        return $this->prefix . '/' . static::SERVICE_NAME . '/' . $path;
    }

    /************************************************
     * Testing GET
     ************************************************/

    public function testDefaultResources()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest($request);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(4, $data[static::$wrapper]);
//        $this->assert( '_schema', $data[static::$wrapper] );
//        $this->assertCount( 3, $data[static::$wrapper] );
//        $this->assertArrayHasKey( '_proc', $data[static::$wrapper] );
//        $this->assertCount( 3, $data[static::$wrapper] );
//        $this->assertArrayHasKey( '_func', $data[static::$wrapper] );
//        $this->assertCount( 3, $data[static::$wrapper] );
    }

    public function testSchemaEmpty()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest($request, Schema::RESOURCE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertEmpty($data[static::$wrapper]);
    }

    public function testProceduresEmpty()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest($request, StoredProcedure::RESOURCE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertEmpty($data[static::$wrapper]);
    }

    public function testFunctionsEmpty()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest($request, StoredFunction::RESOURCE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertEmpty($data[static::$wrapper]);
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

        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Schema::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey('name', $data);
        $this->assertSame(static::TABLE_NAME, $data['name']);
    }

    public function testGetRecordsEmpty()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertEmpty($data[static::$wrapper]);
    }

    public function testCreateRecords()
    {
        $payload = '[
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
	    ]';

        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }
        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(3, $data[static::$wrapper]);
    }

    public function testGetRecordById()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1');
        $data = $rs->getContent();
        $this->assertTrue($data[static::TABLE_ID] == 1);
    }

    public function testGetRecordsByIds()
    {
        $request = new TestServiceRequest(Verbs::GET, [ApiOptions::IDS => '2,3']);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $ids = implode(",", array_column($data[static::$wrapper], static::TABLE_ID));
        $this->assertTrue($ids == "2,3");
    }

    public function testResourceNotFound()
    {
        $request = new TestServiceRequest(Verbs::GET);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/5');
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\NotFoundException', $ex);
            $this->assertEquals(404, $ex->getCode());
        }
    }

    /************************************************
     * Testing POST
     ************************************************/

    public function testCreateRecord()
    {
        $payload = '[{"name":"test4","complete":false}]';
        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }
        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(1, $data[static::$wrapper]);
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

        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(2, $data[static::$wrapper]);
    }

    public function testCreateRecordReturnFields()
    {
        $payload = '[{"name":"test7","complete":true}]';
        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }

        $request = new TestServiceRequest(Verbs::POST, [ApiOptions::FIELDS => 'name,complete']);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(1, $data[static::$wrapper]);
        $this->assertArrayHasKey('name', $data[static::$wrapper][0]);
        $this->assertArrayHasKey('complete', $data[static::$wrapper][0]);
    }

    public function testCreateRecordsWithContinue()
    {
        $payload = '[
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
        ]';

        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }
        $request = new TestServiceRequest(Verbs::POST, [ApiOptions::CONTINUES => true]);
        $request->setContent($payload, DataFormats::JSON);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\BadRequestException', $ex);
            $this->assertEquals(400, $ex->getCode());
            $this->assertContains('Batch Error: Not all records could be created.', $ex->getMessage());
//            $this->assertContains( "Duplicate entry 'test5'", $ex->getMessage() );
        }
    }

    public function testCreateRecordsWithRollback()
    {
        $payload = '[
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
        ]';
        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }

        $request = new TestServiceRequest(Verbs::POST, [ApiOptions::ROLLBACK => true]);
        $request->setContent($payload, DataFormats::JSON);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\InternalServerErrorException', $ex);
            $this->assertEquals(500, $ex->getCode());
            $this->assertContains('All changes rolled back.', $ex->getMessage());
//            $this->assertContains( "Duplicate entry 'test5'", $ex->getMessage() );
        }
    }

    public function testCreateRecordBadRequest()
    {
        $payload = '[{"name":"test1", "complete":true}]';
        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }

        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\InternalServerErrorException', $ex);
            $this->assertEquals(500, $ex->getCode());
            $this->assertContains("duplicate ", $ex->getMessage(), '', true);
        }
    }

    public function testCreateRecordFailNotNullField()
    {
        $payload = '[{"name":null, "complete":true}]';
        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }

        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\BadRequestException', $ex);
            $this->assertEquals(400, $ex->getCode());
            $this->assertContains("Field 'name' can not be NULL.", $ex->getMessage());
        }
    }

    public function testCreateRecordFailMissingRequiredField()
    {
        $payload = '[{"complete":true}]';
        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }

        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\BadRequestException', $ex);
            $this->assertEquals(400, $ex->getCode());
            $this->assertContains("Required field 'name' can not be NULL.", $ex->getMessage());
        }
    }

    /************************************************
     * Testing PUT
     ************************************************/

    public function testPUTRecordById()
    {
        $this->testUpdateRecordById(Verbs::PUT);
    }

    public function testPUTRecordByIds()
    {
        $this->testUpdateRecordByIds(Verbs::PUT);
    }

    public function testPUTRecords()
    {
        $this->testUpdateRecords(Verbs::PUT);
    }

    /************************************************
     * Testing PATCH
     ************************************************/

    public function testUpdateRecordById($verb = Verbs::PATCH)
    {
        $payload = '{"name":"test1Update"}';
        $request = new TestServiceRequest($verb);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1');
//        $this->assertEquals( '{"id":1}', $rs->getContent() );

        $request->setMethod(Verbs::GET);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1');
        } catch (\Exception $ex) {
            $this->assertTrue(false, $ex->getMessage());
        }
    }

    public function testUpdateRecordByIds($verb = Verbs::PATCH)
    {
//        $dColumn = implode( ",", array_column( $ra[static::$wrapper], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra[static::$wrapper], 'label' ) );
//        $this->assertEquals( "unit-test-description,unit-test-description,unit-test-description", $dColumn );
//        $this->assertEquals( "unit-test-label,unit-test-label,unit-test-label", $lColumn );
        $payload = '{"complete":true}';
        $request = new TestServiceRequest($verb, [ApiOptions::IDS => '2,3']);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
//        $this->assertEquals( '{"record":[{"id":2},{"id":3}]}', $rs->getContent() );

        $request->setMethod(Verbs::GET);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        } catch (\Exception $ex) {
            $this->assertTrue(false, $ex->getMessage());
        }
    }

    public function testUpdateRecords($verb = Verbs::PATCH)
    {
//        $this->assertContains( '{"record":[{"id":1},{"id":2},{"id":3}]}', $rs->getContent() );
//        $dColumn = implode( ",", array_column( $ra[static::$wrapper], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra[static::$wrapper], 'label' ) );
//        $this->assertEquals( "unit-test-d1,unit-test-d2,unit-test-d3", $dColumn );
//        $this->assertEquals( "unit-test-l1,unit-test-l2,unit-test-l3", $lColumn );
        $payload = '[
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
        ]';
        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }

        $request = new TestServiceRequest($verb);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(3, $data[static::$wrapper]);
    }

    public function testUpdateRecordsWithFields($verb = Verbs::PATCH)
    {
        $payload = '[{"id": 4, "name":"test4Update","complete":true}]';
        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }

        $request = new TestServiceRequest($verb, [ApiOptions::FIELDS => 'name,complete']);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(1, $data[static::$wrapper]);
        $this->assertArrayHasKey('name', $data[static::$wrapper][0]);
        $this->assertArrayHasKey('complete', $data[static::$wrapper][0]);
//        $this->assertContains( '{"record":[{"label":"unit-test-l1"},{"label":"unit-test-l2"},{"label":"unit-test-l3"}]}', $rs->getContent() );
    }

    public function testUpdateRecordsWithContinue($verb = Verbs::PATCH)
    {
        $payload = '[
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
        ]';
        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }

        $request = new TestServiceRequest($verb, [ApiOptions::CONTINUES => true]);
        $request->setContent($payload, DataFormats::JSON);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\BadRequestException', $ex);
            $this->assertEquals(400, $ex->getCode());
            $this->assertContains('Batch Error: Not all records could be patched.', $ex->getMessage());
//            $this->assertContains( "Duplicate entry 'test5'", $ex->getMessage() );
        }
//        $this->assertContains( '{"error":{"context":{"errors":[1],"record":[{"id":1},', $rs->getContent() );
//        $this->assertContains( ',{"id":3}]}', $rs->getContent() );
//        $this->assertResponseStatus( 400 );
//
//        $result = $this->call( Verbs::GET, $this->buildPath( '_table/todo?ids=1,2,3') );
//        $ra = json_decode( $result->getContent(), true );
//        $dColumn = implode( ",", array_column( $ra[static::$wrapper], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra[static::$wrapper], 'label' ) );
//
//        $this->assertEquals( "unit-test-d1,Local Database 2,unit-test-d3", $dColumn );
//        $this->assertEquals( "unit-test-l1,Database 2,unit-test-l3", $lColumn );
    }

    public function testUpdateRecordsWithRollback($verb = Verbs::PATCH)
    {
        $payload = '[
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
        ]';
        if (static::$wrapper) {
            $payload = '{' . static::$wrapper . ': ' . $payload . '}';
        }

        $request = new TestServiceRequest($verb, [ApiOptions::ROLLBACK => true]);
        $request->setContent($payload, DataFormats::JSON);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\NotFoundException', $ex);
            $this->assertEquals(404, $ex->getCode());
            $this->assertContains('All changes rolled back.', $ex->getMessage());
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
//        $dColumn = implode( ",", array_column( $ra[static::$wrapper], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra[static::$wrapper], 'label' ) );
//
//        $this->assertEquals( "Local Database,Local Database 2,Local Database 3", $dColumn );
//        $this->assertEquals( "Database,Database 2,Database 3", $lColumn );
    }

    /************************************************
     * Testing DELETE
     ************************************************/

    public function testDeleteRecordById()
    {
        $request = new TestServiceRequest(Verbs::DELETE);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1');
//        $this->assertEquals( '{"id":1}', $rs->getContent() );

        $request->setMethod(Verbs::GET);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1');
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\NotFoundException', $ex);
        }
    }

    public function testDeleteRecordByIds()
    {
        $request = new TestServiceRequest(Verbs::DELETE, [ApiOptions::IDS => '2,3']);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
//        $this->assertEquals( '{"record":[{"id":2},{"id":3}]}', $rs->getContent() );

        $request->setMethod(Verbs::GET);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/2');
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\NotFoundException', $ex);
        }

        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/3');
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\NotFoundException', $ex);
        }
    }

    public function testDeleteRecords()
    {
        $payload = '[{"id":4},{"id":5}]';
        $request = new TestServiceRequest(Verbs::DELETE);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
//        $this->assertEquals( '{"record":[{"id":2},{"id":3}]}', $rs->getContent() );

        $request->setMethod(Verbs::GET);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/4');
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\NotFoundException', $ex);
        }

        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/5');
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\NotFoundException', $ex);
        }
    }

    public function testDeleteRecordsWithFields()
    {
        $payload = '[{"id":6},{"id":7}]';
        $request = new TestServiceRequest(Verbs::DELETE, [ApiOptions::FIELDS => 'name']);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
//        $this->assertEquals( '{"record":[{"id":2},{"id":3}]}', $rs->getContent() );

        $request->setMethod(Verbs::GET);
        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/6');
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\NotFoundException', $ex);
        }

        try {
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/7');
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\NotFoundException', $ex);
        }
    }

    public function testDropTable()
    {
        $request = new TestServiceRequest(Verbs::DELETE);
        $rs = $this->service->handleRequest($request, Schema::RESOURCE_NAME . '/' . static::TABLE_NAME);

        $request->setMethod(Verbs::GET);
        try {
            $rs = $this->service->handleRequest($request, Schema::RESOURCE_NAME . '/' . static::TABLE_NAME);
            $this->assertTrue(false);
        } catch (\Exception $ex) {
            $this->assertInstanceOf('\DreamFactory\Core\Exceptions\NotFoundException', $ex);
        }
    }
}
