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
