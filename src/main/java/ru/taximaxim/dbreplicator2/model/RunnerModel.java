/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2013 Technologiya
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package ru.taximaxim.dbreplicator2.model;

import java.util.List;

import javax.persistence.Entity;
import javax.persistence.Table;

import ru.taximaxim.dbreplicator2.replica.Runner;

@Entity
@Table( name = "runners" )
public class RunnerModel implements Runner {

	/**
	 * Идентификатор выполняемого потока
	 */
	private int id;

	/**
	 * Именованный пул-источник
	 */
	private String source;
	
	/**
	 * Именованный целевой пул
	 */
	private String target;
	
	/**
	 * Описание потока исполнителя
	 */
	private String description;
	
	private List<StrategyModel> strategyModels;

	/**
	 * @see RunnerModel#source
	 */
	@Override
	public String getSource() {
		return source;
	}

	/**
	 * @see RunnerModel#target
	 */
	@Override
	public String getTarget() {
		return target;
	}

	/**
	 * @see RunnerModel#id
	 */
	@Override
	public Integer getId() {
		return id;
	}

	/**
	 * @see RunnerModel#description
	 */
	@Override
	public String getDescription() {
		return description;
	}

	/**
	 * @see RunnerModel#strategyModels
	 */
	@Override
	public List<StrategyModel> getStrategies() {
		return strategyModels;
	}
}
