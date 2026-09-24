<?php

namespace DreamFactory\Core\SqlDb\Tests;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\SqlDb\Resources\Table;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

/**
 * Regression for df-core#172: a filter written without parentheses around each
 * condition (a is null OR b is null) must parse the same as the parenthesized
 * form. Previously everything after the first comparison operator was swallowed
 * and the query silently matched the wrong rows.
 *
 * Drives parseFilterString() directly, same approach as FilterDbFunctionTest;
 * IS NULL / IS NOT NULL operators need no value parsing, so no connection.
 */
class BareLogicalFilterTest extends TestCase
{
    protected function parseFilter(string $filter): string
    {
        $table = (new ReflectionClass(Table::class))->newInstanceWithoutConstructor();
        $method = new \ReflectionMethod(Table::class, 'parseFilterString');
        $method->setAccessible(true);

        $fields = [];
        foreach (['a', 'b', 'c'] as $name) {
            $col = new ColumnSchema(['name' => $name]);
            $col->quotedName = '"' . $name . '"';
            $fields[$name] = $col;
        }
        $params = [];

        return $method->invokeArgs($table, [$filter, &$params, $fields, []]);
    }

    public static function cases(): array
    {
        return [
            'OR'        => ['a is null OR b is null', '(a is null) OR (b is null)'],
            'AND'       => ['a is null AND b is not null', '(a is null) AND (b is not null)'],
            'lowercase' => ['a is null or b is null', '(a is null) OR (b is null)'],
            'three'     => ['a is null AND b is null OR c is null', '(a is null) AND (b is null) OR (c is null)'],
            'mixed'     => ['(a is null) AND b is null', '(a is null) AND (b is null)'],
            'wrapped'   => ['(a is null AND b is null)', '((a is null) AND (b is null))'],
            'wrapped+outer' => ['(a is null AND b is null) OR (c is null)', '((a is null) AND (b is null)) OR (c is null)'],
        ];
    }

    #[DataProvider('cases')]
    public function testBareConditionsParseLikeParenthesized(string $bare, string $parenthesized)
    {
        $expected = $this->parseFilter($parenthesized);
        $this->assertStringContainsString('"b"', $expected, 'sanity: parenthesized form covers both fields');
        $this->assertSame($expected, $this->parseFilter($bare));
    }
}
