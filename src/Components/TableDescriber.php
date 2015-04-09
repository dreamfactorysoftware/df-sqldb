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
namespace DreamFactory\Rave\SqlDb\Components;

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
    protected static function mergeTableExtras( $table, $extras = null )
    {
        $out = ArrayUtils::clean( $table );
        $extras = ArrayUtils::clean( $extras );

        $labelInfo = ArrayUtils::get( $extras, '', array() );
        $label = ArrayUtils::get( $labelInfo, 'label' );
        if ( !empty( $label ) )
        {
            $out['label'] = $label;
        }

        $plural = ArrayUtils::get( $labelInfo, 'plural' );
        if ( !empty( $plural ) )
        {
            $out['plural'] = $plural;
        }

        $name_field = ArrayUtils::get( $labelInfo, 'name_field' );
        if ( !empty( $name_field ) )
        {
            $out['name_field'] = $name_field;
        }

        $fields = ArrayUtils::get( $table, 'fields', array() );
        foreach ( $fields as &$field )
        {
            $name = ArrayUtils::get( $field, 'name' );
            $_info = ArrayUtils::get( $extras, $name, array() );
            $field = static::mergeFieldExtras( $field, $_info );
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
    protected static function mergeFieldExtras( $column, $extras = null )
    {
        $out = ArrayUtils::clean( $column );
        $extras = ArrayUtils::clean( $extras );

        $label = ArrayUtils::get( $extras, 'label' );
        if ( !empty( $label ) )
        {
            $out['label'] = $label;
        }

        $validation = json_decode( ArrayUtils::get( $extras, 'validation' ), true );
        if ( !empty( $validation ) && is_string( $validation ) )
        {
            // backwards compatible with old strings
            $validation = array_map( 'trim', explode( ',', $validation ) );
            $validation = array_flip( $validation );
        }
        $out['validation'] = $validation;
        if ( is_array( $validation ) && isset( $validation['api_read_only'] ) )
        {
            $out['required'] = false;
        }

        $picklist = ArrayUtils::get( $extras, 'picklist' );
        $picklist = ( !empty( $picklist ) ) ? explode( "\r", $picklist ) : array();
        $out['value'] = $picklist;

        switch ( ArrayUtils::get( $column, 'type' ) )
        {
            case 'integer':
                if ( isset( $extras['user_id_on_update'] ) )
                {
                    $out['type'] = 'user_id_on_' . ( ArrayUtils::getBool( $extras, 'user_id_on_update' ) ? 'update' : 'create' );
                }

                if ( null !== ArrayUtils::get( $extras, 'user_id' ) )
                {
                    $out['type'] = 'user_id';
                }
                break;

            case 'timestamp':
                if ( isset( $extras['timestamp_on_update'] ) )
                {
                    $out['type'] = 'timestamp_on_' . ( ArrayUtils::getBool( $extras, 'timestamp_on_update' ) ? 'update' : 'create' );
                }
                break;
        }

        return $out;
    }

}