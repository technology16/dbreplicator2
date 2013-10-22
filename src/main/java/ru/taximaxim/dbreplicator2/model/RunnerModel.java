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

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;

import org.hibernate.annotations.Where;

import ru.taximaxim.dbreplicator2.replica.Runner;

@Entity
@Table( name = "runners" )
public class RunnerModel implements Runner {

	/**
	 * Идентификатор выполняемого потока
	 */
	@Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE)
	private Integer id;

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
	
	/**
	 * Список стратегий, которые необхоимо выполнить потоку
	 */
	@OneToMany(mappedBy="runner")
	@Where(clause="isEnabled=true")
	@OrderBy("priority ASC")
	private List<StrategyModel> strategyModels;

	/**
	 * Добавляет стратегию к runner'y
	 * 
	 * @param strategy
	 * @return
	 */
	public List<StrategyModel> addStrategy(StrategyModel strategy) {
		
		List<StrategyModel> strategies = getStrategyModels();
		strategies.add(strategy);
		strategy.setRunner(this);
		
		return strategies;
	}
	
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

	public List<StrategyModel> getStrategyModels() {
		
		if (strategyModels == null)
			strategyModels = new ArrayList<StrategyModel>();
		
		return strategyModels;
	}

	public void setStrategyModels(List<StrategyModel> strategyModels) {
		this.strategyModels = strategyModels;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}
