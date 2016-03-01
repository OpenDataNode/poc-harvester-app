/**
 * Copyright 2015 Peter Goliuan.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package eu.comsode.unifiedviews.plugins.transformer.generatedtorelational;


/**
 * Represents database table column.
 * Contains information about column type, name, size, etc.
 */
public class ColumnDefinition {

    private String columnName;

    private String columnTypeName;

    private int columnType;

    private boolean columnNotNull;

    private int columnSize;

    private String typeClassName;

    public ColumnDefinition(String columnName, String columnTypeName, int columnType, boolean columnNotNull, String typeClass, int columnSize) {
        this.columnName = columnName;
        this.columnTypeName = columnTypeName;
        this.columnType = columnType;
        this.columnNotNull = columnNotNull;
        this.columnSize = columnSize;
        this.typeClassName = typeClass;
    }

    public ColumnDefinition(String columnName, String columnTypeName, int columnType, boolean columnNotNull, String typeClass) {
        this(columnName, columnTypeName, columnType, columnNotNull, typeClass, -1);
    }

    public String getColumnName() {
        return this.columnName;
    }

    public String getColumnTypeName() {
        return this.columnTypeName;
    }

    public int getColumnType() {
        return this.columnType;
    }

    public boolean isNotNull() {
        return this.columnNotNull;
    }

    public int getColumnSize() {
        return this.columnSize;
    }

    public String getTypeClassName() {
        return this.typeClassName;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ColumnDefinition)) {
            return false;
        }
        ColumnDefinition cd = (ColumnDefinition) o;
        if (this.columnName.equals(cd.getColumnName()) && this.columnType == cd.getColumnType()) {
            return true;
        } else {
            return false;
        }
    }
}
