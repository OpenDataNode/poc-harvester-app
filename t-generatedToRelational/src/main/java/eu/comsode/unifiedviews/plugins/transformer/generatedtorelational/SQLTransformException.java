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

public class SQLTransformException extends Exception {
    
    private static final long serialVersionUID = 1665794909987681813L;
    private TransformErrorCode errorCode;
    
    public static enum TransformErrorCode {
        DUPLICATE_COLUMN_NAME, UNSUPPORTED_TYPE, UNKNOWN;
    };
    
    public SQLTransformException(String message, TransformErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode;
    }
    
    public SQLTransformException(String message, Throwable t, TransformErrorCode errorCode) {
        super(message, t);
        this.errorCode = errorCode;
    }
    
    public SQLTransformException(String message, Throwable t) {
        super(message, t);
        this.errorCode = TransformErrorCode.UNKNOWN;
    }
    
    public SQLTransformException(String message) {
        super(message);
        this.errorCode = TransformErrorCode.UNKNOWN;
    }
    
    public TransformErrorCode getErrorCode() {
        return this.errorCode;
    }

}
