<?php

namespace DreamFactory\Core\SqlDb\Tests\Security;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\SqlDb\Resources\Table;
use Illuminate\Database\SQLiteConnection;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use ReflectionMethod;
use ReflectionProperty;

/**
 * role (server-side) row-level-security filter must stay isolated from
 * the client-supplied filter.
 *
 * The old code merged the two filters by TEXT concatenation,
 *   (<client>) AND (<server>),
 * then parsed the combined string. A client filter with an unbalanced paren
 * ("id > 0) OR (id > 0") rewrote the structure so the OR floated above the role
 * filter and leaked every row. A string-valued role filter ("category = 'shelter'")
 * also depended on that single re-parse for its quoting.
 *
 * The fix parses each filter separately and applies each as its own builder
 * where-group, and rejects any group whose emitted SQL has unbalanced parentheses. These tests
 * drive convertFilterToNative() directly against an in-memory SQLite query
 * builder (no live stack, no DreamFactory app boot) and inspect the compiled SQL.
 *
 * Filter strings here contain no "{" so Session::replaceLookups() is a no-op and
 * needs no container.
 */
class RoleFilterIsolationTest extends TestCase
{
    /** Build a Table instance without running its constructor, with a stub parent. */
    private function table(): Table
    {
        $table = (new ReflectionClass(Table::class))->newInstanceWithoutConstructor();

        // parseFilterValue() calls $this->parent->getSchema()->typecastToNative();
        // stub it to return the value unchanged so values flow through unchanged.
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
        $prop->setAccessible(true);
        $prop->setValue($table, $parent);

        return $table;
    }

    /** @return ColumnSchema[] keyed by lowercase field name, as the parser expects. */
    private function fields(): array
    {
        $fields = [];
        foreach (['id', 'category'] as $name) {
            $col = new ColumnSchema(['name' => $name]);
            $col->quotedName = '"' . $name . '"';
            $fields[$name] = $col;
        }

        return $fields;
    }

    /** A real query builder with no server: SQLite in memory, never executed. */
    private function builder()
    {
        $conn = new SQLiteConnection(new \PDO('sqlite::memory:'));

        return $conn->table('products');
    }

    private function convert($builder, $clientFilter, array $ssFilters): void
    {
        $method = new ReflectionMethod(Table::class, 'convertFilterToNative');
        $method->setAccessible(true);
        $method->invoke($this->table(), $builder, $clientFilter, [], $ssFilters, $this->fields());
    }

    /** Role limits to id = 1. */
    private function roleFilter(): array
    {
        return ['filters' => [['name' => 'id', 'operator' => '=', 'value' => 1]]];
    }

    /**
     * The proven breakout payload must be rejected, not merged. Under the old code
     * it produced "(id > 0) OR (id > 0) AND (id = 1)" and leaked every row.
     */
    public function testUnbalancedClientFilterIsRejected(): void
    {
        $this->expectException(BadRequestException::class);
        $this->convert($this->builder(), 'id > 0) OR (id > 0', $this->roleFilter());
    }

    /**
     * A balanced client OR filter must stay trapped inside its own group so the
     * role filter still AND-constrains every row. The OR group is parenthesized
     * and AND-ed with the role group: it cannot float above it.
     */
    public function testBalancedClientOrCannotEscapeRoleFilter(): void
    {
        $builder = $this->builder();
        $this->convert($builder, '(id > 0) OR (id > 0)', $this->roleFilter());

        $sql = strtolower($builder->toSql());

        // Each filter is its own parenthesized group, joined by AND.
        $this->assertStringContainsString(
            '(("id" > ?) or ("id" > ?)) and (("id" = ?))',
            $sql,
            "Role filter must AND-constrain the client OR group; got: $sql"
        );
        // The role predicate is never OR-ed at the top level.
        $this->assertStringNotContainsString('? or ("id" = ?)', $sql);

        $this->assertSame(['0', '0', '1'], array_map('strval', $builder->getBindings()));
    }

    /**
     * A plain legitimate client filter still works and is grouped with the role.
     */
    public function testLegitimateClientFilterStillWorks(): void
    {
        $builder = $this->builder();
        $this->convert($builder, 'id > 2', $this->roleFilter());

        $sql = strtolower($builder->toSql());
        $this->assertStringContainsString('("id" > ?) and (("id" = ?))', $sql);
        $this->assertSame(['2', '1'], array_map('strval', $builder->getBindings()));
    }

    /**
     * A string-valued role filter keeps correct quoting: the value is bound as a
     * parameter ("category" = ?), not inlined as a bare word ("category = shelter"),
     * which previously 500'd whenever a client filter was present.
     */
    public function testStringRoleFilterKeepsQuotingViaBinding(): void
    {
        $builder = $this->builder();
        $ss = ['filters' => [['name' => 'category', 'operator' => '=', 'value' => "'shelter'"]]];

        // No client filter: role filter applied alone.
        $this->convert($builder, '', $ss);

        $sql = $builder->toSql();
        $this->assertStringContainsString('"category" = ?', $sql, "value must be a bound parameter: $sql");
        $this->assertStringNotContainsString('shelter', $sql, 'value must not be inlined into SQL text');
        $this->assertSame(['shelter'], $builder->getBindings(), 'bound value keeps its string content');
    }

    /**
     * The same string role filter still binds correctly when a client filter is
     * present (the condition the report flagged).
     */
    public function testStringRoleFilterKeepsQuotingWithClientFilter(): void
    {
        $builder = $this->builder();
        $ss = ['filters' => [['name' => 'category', 'operator' => '=', 'value' => "'shelter'"]]];

        $this->convert($builder, 'id > 0', $ss);

        $sql = $builder->toSql();
        $this->assertStringContainsString('"category" = ?', $sql);
        $this->assertStringNotContainsString('shelter', $sql);
        $this->assertSame(['0', 'shelter'], array_map('strval', $builder->getBindings()));
    }

    /** Direct coverage of the structural guard. */
    public function testParenthesesBalanceGuard(): void
    {
        $method = new ReflectionMethod(Table::class, 'parenthesesAreBalanced');
        $method->setAccessible(true);
        $check = fn($s) => $method->invoke(null, $s);

        // Breakout attempts: a prefix closes a group it never opened, or parens dangle.
        $this->assertFalse($check('id > 0) OR (id > 0'));
        $this->assertFalse($check('(id > 0'));
        $this->assertFalse($check('id > 0)'));
        $this->assertFalse($check(') OR ('));

        // Balanced fragments, as the parser emits them.
        $this->assertTrue($check('"id" = ?'));
        $this->assertTrue($check('("id" > ?) OR ("id" = ?)'));
        $this->assertTrue($check('"id" IN (?,?,?)'));
        $this->assertTrue($check('"created" > (NOW())'));
    }

    /**
     * Quotes must not hide a breakout. The parser splits on ") OR (" inside
     * quotes too, so these inputs emit an unbalanced fragment. They leaked every
     * row when the guard scanned the input and skipped quoted text.
     */
    public function testQuoteHiddenBreakoutIsRejected(): void
    {
        foreach ([
            "category = ') OR (id > 0) OR (category = '",
            'category = "a) OR (id > 0) OR (category = b"',
            "category like ') OR (id > 0) OR (category like '",
        ] as $payload) {
            try {
                $this->convert($this->builder(), $payload, $this->roleFilter());
                $this->fail("Breakout payload was accepted: $payload");
            } catch (BadRequestException $e) {
                $this->assertStringContainsString('Unbalanced parentheses', $e->getMessage());
            }
        }
    }

    /**
     * Values with a lone apostrophe or with parentheses inside quotes are
     * ordinary filters and must still work.
     */
    public function testApostropheAndQuotedParenValuesStillWork(): void
    {
        foreach (["category = O'Brien" => "O'Brien", "category = 'x(y)'" => 'x(y)', "category = 'x)'" => 'x)'] as $filter => $bound) {
            $builder = $this->builder();
            $this->convert($builder, $filter, $this->roleFilter());
            $this->assertSame([$bound, '1'], array_map('strval', $builder->getBindings()), $filter);
            $this->assertStringContainsString('("category" = ?) and (("id" = ?))', strtolower($builder->toSql()), $filter);
        }
    }
}
