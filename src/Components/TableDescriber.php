<?php
namespace DreamFactory\Core\SqlDb\Components;

use DreamFactory\Core\Utility\ResourcesWrapper;
use DreamFactory\Library\Utility\ArrayUtils;

trait TableDescriber
{
    /**
     * @param array $table
     * @param array $extras
     *
     * @throws \Exception
     * @return array
     */
    protected static function mergeTableExtras($table, $extras = null)
    {
        $out = ArrayUtils::clean($table);
        $extras = ArrayUtils::clean($extras);

        $labelInfo = ArrayUtils::get($extras, '', []);
        $label = ArrayUtils::get($labelInfo, 'label');
        if (!empty($label)) {
            $out['label'] = $label;
        }

        $plural = ArrayUtils::get($labelInfo, 'plural');
        if (!empty($plural)) {
            $out['plural'] = $plural;
        }

        $name_field = ArrayUtils::get($labelInfo, 'name_field');
        if (!empty($name_field)) {
            $out['name_field'] = $name_field;
        }

        $fields = ArrayUtils::get($table, 'fields', []);
        foreach ($fields as &$field) {
            $name = ArrayUtils::get($field, 'name');
            $info = ArrayUtils::get($extras, $name, []);
            $field = static::mergeFieldExtras($field, $info);
        }

        return $out;
    }

    /**
     * @param array $column
     * @param array $extras
     *
     * @throws \Exception
     * @return array
     */
    protected static function mergeFieldExtras($column, $extras = null)
    {
        $out = ArrayUtils::clean($column);
        $extras = ArrayUtils::clean($extras);

        $label = ArrayUtils::get($extras, 'label');
        if (!empty($label)) {
            $out['label'] = $label;
        }

        $validation = json_decode(ArrayUtils::get($extras, 'validation'), true);
        if (!empty($validation) && is_string($validation)) {
            // backwards compatible with old strings
            $validation = array_map('trim', explode(',', $validation));
            $validation = array_flip($validation);
        }
        $out['validation'] = $validation;
        if (is_array($validation) && isset($validation['api_read_only'])) {
            $out['required'] = false;
        }

        $picklist = ArrayUtils::get($extras, 'picklist');
        $picklist = (!empty($picklist)) ? explode("\r", $picklist) : [];
        $out['value'] = $picklist;

        switch (ArrayUtils::get($column, 'type')) {
            case 'integer':
                if (isset($extras['user_id_on_update'])) {
                    $out['type'] =
                        'user_id_on_' . (ArrayUtils::getBool($extras, 'user_id_on_update') ? 'update' : 'create');
                }

                if (null !== ArrayUtils::get($extras, 'user_id')) {
                    $out['type'] = 'user_id';
                }
                break;

            case 'timestamp':
                if (isset($extras['timestamp_on_update'])) {
                    $out['type'] =
                        'timestamp_on_' . (ArrayUtils::getBool($extras, 'timestamp_on_update') ? 'update' : 'create');
                }
                break;
        }

        return $out;
    }

    public static function getApiDocCommonModels()
    {
        $wrapper = ResourcesWrapper::getWrapper();

        return [
            'TableSchemas'  => [
                'id'         => 'TableSchemas',
                'properties' => [
                    $wrapper => [
                        'type'        => 'Array',
                        'description' => 'An array of table definitions.',
                        'items'       => [
                            '$ref' => 'TableSchema',
                        ],
                    ],
                ],
            ],
            'TableSchema'   => [
                'id'         => 'TableSchema',
                'properties' => [
                    'name'        => [
                        'type'        => 'string',
                        'description' => 'Identifier/Name for the table.',
                    ],
                    'label'       => [
                        'type'        => 'string',
                        'description' => 'Displayable singular name for the table.',
                    ],
                    'plural'      => [
                        'type'        => 'string',
                        'description' => 'Displayable plural name for the table.',
                    ],
                    'primary_key' => [
                        'type'        => 'string',
                        'description' => 'Field(s), if any, that represent the primary key of each record.',
                    ],
                    'name_field'  => [
                        'type'        => 'string',
                        'description' => 'Field(s), if any, that represent the name of each record.',
                    ],
                    'field'       => [
                        'type'        => 'Array',
                        'description' => 'An array of available fields in each record.',
                        'items'       => [
                            '$ref' => 'FieldSchema',
                        ],
                    ],
                    'related'     => [
                        'type'        => 'Array',
                        'description' => 'An array of available relationships to other tables.',
                        'items'       => [
                            '$ref' => 'RelatedSchema',
                        ],
                    ],
                ],
            ],
            'FieldSchema'   => [
                'id'         => 'FieldSchema',
                'properties' => [
                    'name'               => [
                        'type'        => 'string',
                        'description' => 'The API name of the field.',
                    ],
                    'label'              => [
                        'type'        => 'string',
                        'description' => 'The displayable label for the field.',
                    ],
                    'type'               => [
                        'type'        => 'string',
                        'description' => 'The DSP abstract data type for this field.',
                    ],
                    'db_type'            => [
                        'type'        => 'string',
                        'description' => 'The native database type used for this field.',
                    ],
                    'length'             => [
                        'type'        => 'integer',
                        'format'      => 'int32',
                        'description' => 'The maximum length allowed (in characters for string, displayed for numbers).',
                    ],
                    'precision'          => [
                        'type'        => 'integer',
                        'format'      => 'int32',
                        'description' => 'Total number of places for numbers.',
                    ],
                    'scale'              => [
                        'type'        => 'integer',
                        'format'      => 'int32',
                        'description' => 'Number of decimal places allowed for numbers.',
                    ],
                    'default_value'      => [
                        'type'        => 'string',
                        'description' => 'Default value for this field.',
                    ],
                    'required'           => [
                        'type'        => 'boolean',
                        'description' => 'Is a value required for record creation.',
                    ],
                    'allow_null'         => [
                        'type'        => 'boolean',
                        'description' => 'Is null allowed as a value.',
                    ],
                    'fixed_length'       => [
                        'type'        => 'boolean',
                        'description' => 'Is the length fixed (not variable).',
                    ],
                    'supports_multibyte' => [
                        'type'        => 'boolean',
                        'description' => 'Does the data type support multibyte characters.',
                    ],
                    'auto_increment'     => [
                        'type'        => 'boolean',
                        'description' => 'Does the integer field value increment upon new record creation.',
                    ],
                    'is_primary_key'     => [
                        'type'        => 'boolean',
                        'description' => 'Is this field used as/part of the primary key.',
                    ],
                    'is_foreign_key'     => [
                        'type'        => 'boolean',
                        'description' => 'Is this field used as a foreign key.',
                    ],
                    'ref_table'          => [
                        'type'        => 'string',
                        'description' => 'For foreign keys, the referenced table name.',
                    ],
                    'ref_fields'         => [
                        'type'        => 'string',
                        'description' => 'For foreign keys, the referenced table field name.',
                    ],
                    'validation'         => [
                        'type'        => 'Array',
                        'description' => 'validations to be performed on this field.',
                        'items'       => [
                            'type' => 'string',
                        ],
                    ],
                    'value'              => [
                        'type'        => 'Array',
                        'description' => 'Selectable string values for client menus and picklist validation.',
                        'items'       => [
                            'type' => 'string',
                        ],
                    ],
                ],
            ],
            'RelatedSchema' => [
                'id'         => 'RelatedSchema',
                'properties' => [
                    'name'      => [
                        'type'        => 'string',
                        'description' => 'Name of the relationship.',
                    ],
                    'type'      => [
                        'type'        => 'string',
                        'description' => 'Relationship type - belongs_to, has_many, many_many.',
                    ],
                    'ref_table' => [
                        'type'        => 'string',
                        'description' => 'The table name that is referenced by the relationship.',
                    ],
                    'ref_fields' => [
                        'type'        => 'string',
                        'description' => 'The field name that is referenced by the relationship.',
                    ],
                    'join'      => [
                        'type'        => 'string',
                        'description' => 'The intermediate joining table used for many_many relationships.',
                    ],
                    'field'     => [
                        'type'        => 'string',
                        'description' => 'The current table field that is used in the relationship.',
                    ],
                ],
            ],
        ];
    }
}