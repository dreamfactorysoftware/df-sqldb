<?php

namespace DreamFactory\Core\SqlDb\Tests;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\SqlDb\Resources\Table;
use Illuminate\Database\SQLiteConnection;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use ReflectionMethod;
use ReflectionProperty;

/**
 * Filters that compare a column with one of the recognized no-argument SQL
 * functions, written bare (born < NOW()) or in parentheses (born < (NOW())),
 * alone or inside AND/OR groups, from the client or from a role filter.
 *
 * The parser used to trim every "(" and ")" from both ends of a condition before
 * finding the operator, which cut the function's own "()" off the value and
 * emitted "born" < ?() for both forms. The function now stays with the value and
 * is used as is; every other value is still bound as a parameter.
 *
 * Drives parseFilterString() and convertFilterToNative() directly with a stub
 * parent service and an in-memory SQLite query builder that is never executed.
 * Filter strings contain no "{", so Session::replaceLookups() needs no container.
 */
class FilterFunctionValueTest extends TestCase
{
    private function table(): Table
    {
        $table = (new ReflectionClass(Table::class))->newInstanceWithoutConstructor();

        // parseFilterValue() calls $this->parent->getSchema()->typecastToNative();
        // the stub returns the value unchanged.
        $schema = new class {
            public function typecastToNative($value, $info)
            {
                return $value;
            }
        };
        $parent = new class($schema) {
            private $schema;

            public function __construct($schema)
            {
                $this->schema = $schema;
            }

            public function getSchema()
            {
                return $this->schema;
            }
        };

        $prop = new ReflectionProperty(Table::class, 'parent');
        $prop->setValue($table, $parent);

        return $table;
    }

    /** @return ColumnSchema[] keyed by lowercase field name, as the parser expects. */
    private function fields(): array
    {
        $fields = [];
        foreach (['id', 'born', 'name'] as $name) {
            $col = new ColumnSchema(['name' => $name]);
            $col->quotedName = '"' . $name . '"';
            $fields[$name] = $col;
        }

        return $fields;
    }

    /** @return array{0: string, 1: array} emitted SQL fragment and bound values */
    private function parse(string $filter): array
    {
        $method = new ReflectionMethod(Table::class, 'parseFilterString');
        $params = [];
        $sql = $method->invokeArgs($this->table(), [$filter, &$params, $this->fields(), []]);

        return [$sql, array_map('strval', $params)];
    }

    private function builder()
    {
        return (new SQLiteConnection(new \PDO('sqlite::memory:')))->table('people');
    }

    private function convert($builder, string $clientFilter, array $ssFilters = []): void
    {
        $method = new ReflectionMethod(Table::class, 'convertFilterToNative');
        $method->invoke($this->table(), $builder, $clientFilter, [], $ssFilters, $this->fields());
    }

    public static function functionFilters(): array
    {
        return [
            'bare'                    => ['born < NOW()', '"born" < NOW()', []],
            'parenthesized value'     => ['born < (NOW())', '"born" < (NOW())', []],
            'wrapped bare'            => ['(born < NOW())', '("born" < NOW())', []],
            'wrapped parenthesized'   => ['(born < (NOW()))', '("born" < (NOW()))', []],
            'lowercase'               => ['born <= now()', '"born" <= now()', []],
            'other function'          => ['born >= CURRENT_DATE()', '"born" >= CURRENT_DATE()', []],
            'and group'               => ['(born < NOW()) and (id > 2)', '("born" < NOW()) AND ("id" > ?)', ['2']],
            'or group, function last' => ['(id = 1) or (born < (NOW()))', '("id" = ?) OR ("born" < (NOW()))', ['1']],
            'bare and/or'             => ['born < NOW() AND id > 2', '("born" < NOW()) AND ("id" > ?)', ['2']],
            'nested groups'           => [
                '((born < NOW()) and (id > 2)) or (id = 1)',
                '(("born" < NOW()) AND ("id" > ?)) OR ("id" = ?)',
                ['2', '1'],
            ],
            'nested, function at end' => [
                '(id = 1) or ((id > 2) and (born < (NOW())))',
                '("id" = ?) OR (("id" > ?) AND ("born" < (NOW())))',
                ['1', '2'],
            ],
            'not'                     => ['not (born < NOW())', 'NOT("born" < NOW())', []],
            'value list'              => ['born in (NOW(), CURDATE())', '"born" IN (NOW(),CURDATE())', []],
        ];
    }

    #[DataProvider('functionFilters')]
    public function testFunctionValueKeepsItsParentheses(string $filter, string $expectedSql, array $expectedParams): void
    {
        [$sql, $params] = $this->parse($filter);

        $this->assertSame($expectedSql, $sql);
        $this->assertSame($expectedParams, $params);
    }

    /** Anything that is not exactly one of the recognized functions is bound, never emitted. */
    public static function boundValues(): array
    {
        return [
            'quoted function name'  => ["born < 'NOW()'", 'NOW()'],
            'function with args'    => ['born < NOW(1)', 'NOW(1)'],
            'expression'            => ['born < NOW() + 1', 'NOW() + 1'],
            'parenthesized sum'     => ['id = (1 + 1)', '(1 + 1)'],
            'unlisted function'     => ['born < LOCALTIMESTAMP()', 'LOCALTIMESTAMP()'],
            'double parentheses'    => ['born < ((NOW()))', '((NOW()))'],
            'plain parenthesized'   => ['name = (abc)', '(abc)'],
        ];
    }

    #[DataProvider('boundValues')]
    public function testOtherValuesAreBound(string $filter, string $bound): void
    {
        [$sql, $params] = $this->parse($filter);

        $this->assertMatchesRegularExpression('/^"(born|id|name)" [<=]+ \?$/', $sql);
        $this->assertSame([$bound], $params);
    }

    /** Filters without functions parse exactly as before. */
    public static function unchangedFilters(): array
    {
        return [
            ['id in (1,2,3)', '"id" IN (?,?,?)', ['1', '2', '3']],
            ["(name = 'a(b')", '("name" = ?)', ['a(b']],
            ["(id > 1) and ((name = 'a(b'))", '("id" > ?) AND (("name" = ?))', ['1', 'a(b']],
            ["name like '%(x)%'", '"name" LIKE ?', ['%(x)%']],
            ['((id = 1) or (id = 2)) and (id > 0)', '(("id" = ?) OR ("id" = ?)) AND ("id" > ?)', ['1', '2', '0']],
        ];
    }

    #[DataProvider('unchangedFilters')]
    public function testFiltersWithoutFunctionsAreUnchanged(string $filter, string $expectedSql, array $expectedParams): void
    {
        [$sql, $params] = $this->parse($filter);

        $this->assertSame($expectedSql, $sql);
        $this->assertSame($expectedParams, $params);
    }

    /** Client and role filters stay separate groups when either uses a function. */
    public function testClientFunctionFilterWithRoleFilter(): void
    {
        $builder = $this->builder();
        $this->convert($builder, 'born < (NOW())', ['filters' => [['name' => 'id', 'operator' => '>', 'value' => '2']]]);

        $this->assertStringContainsString('where ("born" < (NOW())) and (("id" > ?))', $builder->toSql());
        $this->assertSame(['2'], array_map('strval', $builder->getBindings()));
    }

    public function testRoleFilterWithFunctionValue(): void
    {
        $builder = $this->builder();
        $ss = ['filter_op' => 'OR', 'filters' => [
            ['name' => 'born', 'operator' => '<', 'value' => 'NOW()'],
            ['name' => 'id', 'operator' => '=', 'value' => '1'],
        ]];
        $this->convert($builder, 'id > 0', $ss);

        $this->assertStringContainsString('where ("id" > ?) and (("born" < NOW()) OR ("id" = ?))', $builder->toSql());
        $this->assertSame(['0', '1'], array_map('strval', $builder->getBindings()));
    }

    /** Unbalanced input is still refused as a whole group. */
    public function testUnbalancedFunctionFilterIsRejected(): void
    {
        foreach (['born < NOW())', '((born < NOW())', 'born < NOW()) OR (id > 0'] as $filter) {
            try {
                $this->convert($this->builder(), $filter);
                $this->fail("Accepted: $filter");
            } catch (BadRequestException $e) {
                $this->assertStringContainsString('Unbalanced parentheses', $e->getMessage(), $filter);
            }
        }
    }

    public function testSplitOuterParentheses(): void
    {
        $method = new ReflectionMethod(Table::class, 'splitOuterParentheses');
        $split = fn($s) => $method->invoke(null, $s);

        $this->assertSame([null, 'id = 1', null], $split('id = 1'));
        $this->assertSame(['((', 'id = 1', ')'], $split('((id = 1)'));
        $this->assertSame([null, 'born < NOW(', '))'], $split('born < NOW())'));
        $this->assertSame(['(', 'born < (NOW(', ')))'], $split('(born < (NOW()))'));
        $this->assertSame(['(', '', ')'], $split('()'));
    }

    /** An empty pair of parentheses is refused as an unparsable filter. */
    public function testEmptyParenthesesAreRefused(): void
    {
        $this->expectException(BadRequestException::class);
        $this->parse('()');
    }
}
