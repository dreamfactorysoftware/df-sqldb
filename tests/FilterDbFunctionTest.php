<?php

namespace DreamFactory\Core\SqlDb\Tests;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\SqlDb\Resources\Table;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

/**
 * Regression: a column carrying a FILTER db_function must produce a WHERE
 * fragment, not a fatal.
 *
 * parseFilterString() builds its fragment by string concatenation:
 *
 *     $out .= " $sqlOp";
 *     $out .= (isset($value) ? " $value" : null);
 *
 * The FILTER branch used to seed $out with
 * `$this->parent->getConnection()->raw($function)`. Illuminate's
 * Query\Expression dropped __toString() in Laravel 10, so the first
 * concatenation threw
 *
 *     Object of class Illuminate\Database\Query\Expression
 *     could not be converted to string
 *
 * turning any filter on such a column into a 500 - e.g. a column with
 * upper()/COLLATE applied for case-insensitive search, which is the usual
 * reason to configure one.
 *
 * The test drives parseFilterString() directly rather than through a live
 * connection. It filters with IS NULL on purpose: requiresNoValue() operators
 * skip parseFilterValue(), which is the only part of this method that needs a
 * parent service, so the FILTER branch can be exercised with no database and
 * no mocking.
 */
class FilterDbFunctionTest extends TestCase
{
    protected function parseFilter(string $filter, ColumnSchema $column): string
    {
        // No constructor: parseFilterString() needs none of the service wiring,
        // and building it would drag in a real connection.
        $table = (new ReflectionClass(Table::class))->newInstanceWithoutConstructor();

        $method = new \ReflectionMethod(Table::class, 'parseFilterString');
        $method->setAccessible(true);

        $params = [];

        return $method->invokeArgs(
            $table,
            [$filter, &$params, [$column->getName() => $column], []]
        );
    }

    protected function column(string $name, ?array $dbFunction = null): ColumnSchema
    {
        $column = new ColumnSchema(['name' => $name]);
        $column->quotedName = '"' . $name . '"';
        if ($dbFunction !== null) {
            $column->dbFunction = $dbFunction;
        }

        return $column;
    }

    public function testFilterOnColumnWithDbFunctionBuildsSql()
    {
        $column = $this->column('last_name', [
            ['use' => ['FILTER'], 'function' => 'upper(last_name)'],
        ]);

        $sql = $this->parseFilter('last_name is null', $column);

        $this->assertIsString($sql);
        $this->assertStringContainsString('upper(last_name)', $sql);
    }

    public function testFilterOnPlainColumnStillUsesQuotedName()
    {
        $column = $this->column('last_name');

        $sql = $this->parseFilter('last_name is null', $column);

        $this->assertIsString($sql);
        $this->assertStringContainsString('"last_name"', $sql);
    }
}
