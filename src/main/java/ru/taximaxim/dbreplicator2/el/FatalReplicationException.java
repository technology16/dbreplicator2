/**
 * 
 */
package ru.taximaxim.dbreplicator2.el;

/**
 * Ошибка репликации, поле которой продолжать не имеет смысла и надо рестартовать.
 * 
 * Например:
 *   - обрыв соединения
 *   - ошибка в настройках
 *   - ошибка инициализации стратегии
 * 
 * @author volodin_aa
 *
 */
public class FatalReplicationException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public FatalReplicationException() {
    }

    /**
     * Ошибка репликации
     * 
     * @param message
     */
    public FatalReplicationException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public FatalReplicationException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public FatalReplicationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public FatalReplicationException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
