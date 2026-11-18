<?php

namespace DreamFactory\Core\SqlDb\Tests;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\SqlDb\Resources\Table;
use Illuminate\Database\SQLiteConnection;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use ReflectionMethod;
use ReflectionProperty;

/**
 * Requests by id (GET ?ids=, PATCH and DELETE of records by id, and their batch
 * forms) apply the role filter as its own nested WHERE group, as requests by
 * filter do.
 *
 * Runs against an in-memory SQLite table through the same builder calls that
 * addToTransaction() (where id = ?) and commitTransaction() (where id in (...))
 * make, and through truncateTable(). Filter strings contain no "{", so
 * Session::replaceLookups() needs no container.
 */
class RoleFilterByIdTest extends TestCase
{
    /** @var SQLiteConnection */
    private $connection;

    /** OR role filter: (city = San Francisco) OR (score > 60) */
    private const OR_FILTER = ['filter_op' => 'OR', 'filters' => [
        ['name' => 'city', 'operator' => '=', 'value' => 'San Francisco'],
        ['name' => 'score', 'operator' => '>', 'value' => '60'],
    ]];

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = new SQLiteConnection(new \PDO('sqlite::memory:'));
        $this->connection->statement('create table people (id integer primary key, city text, score integer)');
        $this->connection->table('people')->insert([
            ['id' => 1, 'city' => 'Boston', 'score' => 10],
            ['id' => 2, 'city' => 'San Francisco', 'score' => 55],
            ['id' => 3, 'city' => 'Zurich', 'score' => 70],
            ['id' => 4, 'city' => 'Boston', 'score' => 20],
            ['id' => 5, 'city' => 'Boston', 'score' => 90],
            ['id' => 7, 'city' => 'San Francisco', 'score' => 65],
        ]);
    }

    private function table(): Table
    {
        $table = (new ReflectionClass(Table::class))->newInstanceWithoutConstructor();

        $schema = new class {
            public function typecastToNative($value, $info)
            {
                return $value;
            }
        };
        $parent = new class($schema, $this->connection) {
            private $schema;
            private $connection;

            public function __construct($schema, $connection)
            {
                $this->schema = $schema;
                $this->connection = $connection;
            }

            public function getSchema()
            {
                return $this->schema;
            }

            public function getConnection()
            {
                return $this->connection;
            }

            public function getTableSchema($name)
            {
                return (object)['internalName' => $name];
            }
        };
        (new ReflectionProperty(Table::class, 'parent'))->setValue($table, $parent);

        $fields = [];
        foreach (['id', 'city', 'score'] as $name) {
            $col = new ColumnSchema(['name' => $name]);
            $col->quotedName = '"' . $name . '"';
            $fields[$name] = $col;
        }
        (new ReflectionProperty(Table::class, 'tableFieldsInfo'))->setValue($table, $fields);

        return $table;
    }

    private function applyServerFilter($builder, array $ssFilters): void
    {
        (new ReflectionMethod(Table::class, 'applyServerFilterGroup'))->invoke($this->table(), $builder, $ssFilters);
    }

    private function ids($builder): array
    {
        return array_map('intval', $builder->orderBy('id')->pluck('id')->all());
    }

    public function testRoleFilterIsItsOwnGroup(): void
    {
        $builder = $this->connection->table('people')->whereIn('id', [1]);
        $this->applyServerFilter($builder, self::OR_FILTER);

        $this->assertSame(
            'select * from "people" where "id" in (?) and (("city" = ?) OR ("score" > ?))',
            $builder->toSql()
        );
        $this->assertSame(['1', 'San Francisco', '60'], array_map('strval', $builder->getBindings()));
    }

    public function testSingleIdWithOrRoleFilter(): void
    {
        // addToTransaction(): where id = ?
        foreach ([1 => [], 3 => [3], 4 => [], 7 => [7]] as $id => $expected) {
            $builder = $this->connection->table('people')->where('id', $id);
            $this->applyServerFilter($builder, self::OR_FILTER);
            $this->assertSame($expected, $this->ids($builder), "id $id");
        }
    }

    public function testIdListWithOrRoleFilter(): void
    {
        // commitTransaction(): where id in (...)
        $builder = $this->connection->table('people')->whereIn('id', [1, 3, 5]);
        $this->applyServerFilter($builder, self::OR_FILTER);
        $this->assertSame([3, 5], $this->ids($builder));

        $builder = $this->connection->table('people')->whereIn('id', [1, 4]);
        $this->applyServerFilter($builder, self::OR_FILTER);
        $this->assertSame([], $this->ids($builder));
    }

    public function testUpdateAndDeleteByIdWithOrRoleFilter(): void
    {
        $builder = $this->connection->table('people')->where('id', 1);
        $this->applyServerFilter($builder, self::OR_FILTER);
        $this->assertSame(0, $builder->update(['score' => 0]));

        $builder = $this->connection->table('people')->whereIn('id', [1, 4]);
        $this->applyServerFilter($builder, self::OR_FILTER);
        $this->assertSame(0, $builder->delete());

        $builder = $this->connection->table('people')->whereIn('id', [3, 4]);
        $this->applyServerFilter($builder, self::OR_FILTER);
        $this->assertSame(1, $builder->delete());

        $this->assertSame([1, 2, 4, 5, 7], $this->ids($this->connection->table('people')));
        $this->assertSame([10, 55, 20, 90, 65], array_map(
            'intval',
            $this->connection->table('people')->orderBy('id')->pluck('score')->all()
        ));
    }

    public function testAndRoleFilterAndNoRoleFilter(): void
    {
        $and = ['filter_op' => 'AND', 'filters' => [['name' => 'id', 'operator' => '>', 'value' => '2']]];
        $builder = $this->connection->table('people')->whereIn('id', [1, 3, 5]);
        $this->applyServerFilter($builder, $and);
        $this->assertSame([3, 5], $this->ids($builder));

        foreach ([[], null] as $none) {
            $builder = $this->connection->table('people')->whereIn('id', [1, 3]);
            $this->applyServerFilter($builder, $none ?? []);
            $this->assertSame('select * from "people" where "id" in (?, ?)', $builder->toSql());
            $this->assertSame([1, 3], $this->ids($builder));
        }
    }

    public function testRoleFilterWithUnbalancedValueIsRefused(): void
    {
        $ss = ['filters' => [['name' => 'city', 'operator' => '=', 'value' => 'x)']]];
        $builder = $this->connection->table('people')->where('id', 1);

        $this->expectException(BadRequestException::class);
        $this->applyServerFilter($builder, $ss);
    }

    public function testTruncateWithRoleFilterUsesGroupedCondition(): void
    {
        $this->assertSame(['success' => true], $this->table()->truncateTable('people', ['ss_filters' => self::OR_FILTER]));
        $this->assertSame([1, 4], $this->ids($this->connection->table('people')));
    }
}
