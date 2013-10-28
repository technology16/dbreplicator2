/**
 * Функции для конструирования запросов
 * 
 */
package ru.taximaxim.dbreplicator2.jdbc;

import java.util.ArrayList;
import java.util.List;

/**
 * @author volodin_aa
 *
 */
public final class QueryConstructors {
	/**
	 * Сиглетон 
	 */
	private QueryConstructors() {
		
	}

	/**
	 * Строит строку из элементов списка, разделенных разделителем delimiter
	 * 
	 * @param list список объектов
	 * @param delimiter разделитель
	 * @return строка из элементов списка, разделенных разделителем delimiter 
	 */
	public static String listToString(List<?> list, String delimiter){
		StringBuffer result = new StringBuffer();
		
		boolean setComma = false;
		for (Object val: list){
			if (setComma) {
				result.append(delimiter);
			} else {
				setComma = true;			
			}
			result.append(val);
		}
		
		return result.toString();
	}

	/**
	 * Строит строку из элементов списка с добавленным postfix в конце и разделенных разделителем delimiter 
	 * 
	 * @param list список объектов
	 * @param delimiter разделитель
	 * @param postfix строка для добавления после строки элемента
	 * @return строка из элементов списка с добавленным postfix в конце и разделенных разделителем delimiter
	 */
	public static String listToString(List<?> list, String delimiter, String postfix){
		StringBuffer result = new StringBuffer();
		
		boolean setComma = false;
		for (Object val: list){
			if (setComma) {
				result.append(delimiter);
			} else {
				setComma = true;			
			}
			result.append(val).append(postfix);
		}
		
		return result.toString();
	}
	
	/**
	 * Строит список вопрос для передачи экранированых параметров
	 * 
	 * @param colsList список колонок
	 * @return список вопрос для передачи экранированых параметров
	 */
	public static List<String> questionMarks(List<?> colsList){
		List<String> result = new ArrayList<String>();
		int colsListSize = colsList.size();
		for (int i = 0; i<colsListSize; i++) {
			result.add("?");
		}
		
		return result;
	}
	
	/**
	 * Генерирует строку запроса для вставки данных
	 * 
	 * @param tableName имя целевой таблицы
	 * @param colsList список колонок
	 * @return строка запроса для вставки данных
	 */
	public static String constructInsertQuery(String tableName, List<String> colsList){
		StringBuffer insertQuery = new StringBuffer()
		.append("INSERT INTO ")
		.append(tableName)
		.append("(")
		.append(listToString(colsList, ", "))
		.append(") VALUES (")
		.append(listToString(questionMarks(colsList), ", "))
		.append(")");

		return insertQuery.toString();
	}

    /**
     * Генерирует строку запроса следующего вида:
     * INSERT INTO <table>(<cols>) SELECT (<questionsMarks>)
     * Это позволяет создавать запросы на вставку по условию
     * 
     * @param tableName имя целевой таблицы
     * @param colsList список колонок
     * @return строка запроса вставки из запроса выборки
     */
    public static String constructInsertSelectQuery(String tableName, List<String> colsList){
        StringBuffer insertQuery = new StringBuffer()
        .append("INSERT INTO ")
        .append(tableName)
        .append("(")
        .append(listToString(colsList, ", "))
        .append(") ")
        .append(constructSelectQuery(questionMarks(colsList)));

        return insertQuery.toString();
    }

	/**
	 * Создает строку запроса на выборку данных
	 * 
	 * @param colsList список колонок для вставки
	 * @return строкf запроса на выборку данных
	 */
	public static String constructSelectQuery(List<String> colsList){
		StringBuffer query = new StringBuffer()
		.append("SELECT ")
		.append(listToString(colsList, ", "));

		return query.toString();
	}

	/**
	 * Создает строку запроса на выборку данных из таблицы
	 * 
	 * @param tableName имя целевой таблицы
	 * @param colsList список колонок
	 * @return строка запроса на выборку данных из таблицы
	 */
	public static String constructSelectQuery(String tableName, List<String> colsList){
		StringBuffer query = new StringBuffer(constructSelectQuery(colsList))
		.append(" FROM ")		
		.append(tableName);

		return query.toString();
	}

	/**
	 * Создает строку запроса на выборку данных из таблицы с условием
	 * 
	 * @param tableName имя целевой таблицы
	 * @param colsList список колонок
	 * @param whereList список колонок условия
	 * @return строка запроса на выборку данных из таблицы с условием
	 */
	public static String constructSelectQuery(String tableName, List<String> colsList, List<String> whereList){
		StringBuffer query = new StringBuffer(constructSelectQuery(tableName, colsList))
		.append(" WHERE ")
		.append(listToString(whereList, " AND ", "=?"));

		return query.toString();
	}

	/**
	 * Создает строку запроса на выборку данных из таблицы с условием
	 * 
	 * @param tableName имя целевой таблицы
	 * @param colsList список колонок
	 * @param where условие
	 * @return строка запроса на выборку данных из таблицы с условием
	 */
	public static String constructSelectQuery(String tableName, List<String> colsList, String where){
		StringBuffer query = new StringBuffer(constructSelectQuery(tableName, colsList))
		.append(" WHERE ")
		.append(where);

		return query.toString();
	}
	
	/**
	 * Создает строку запроса на удаление данных из таблицы с условием
	 * 
	 * @param tableName имя целевой таблицы
	 * @param whereList список колонок условия
	 * @return
	 */
	public static String constructDeleteQuery(String tableName, List<String> whereList){
		StringBuffer query = new StringBuffer()
		.append("DELETE FROM ")		
		.append(tableName)
		.append(" WHERE ")
		.append(listToString(whereList, " AND ", "=?"));

		return query.toString();
	}
	
}
