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

import java.io.Serializable;
import javax.persistence.*;

/**
 * Класс, описывающий составной ключ таблицы стратегий
 * @author petrov_im
 *
 */
@Embeddable
public class StrategyKey implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Column(name = "id")
    private Integer id;
    
    @ManyToOne
    @JoinColumn(name = "id_runner")
    private RunnerModel runner;
    
    public StrategyKey() {}
    
    public Integer getId() {
        return id;
    }
    
    public RunnerModel getRunner() {
        return runner;
    }
    
    public void setId(Integer id) {
        this.id = id;
    }
    
    public void setRunner(RunnerModel runner) {
        this.runner = runner;
    }
    
    @Override
    public boolean equals(Object object) {
        if ((object == null) || !(object instanceof StrategyKey)) {
            return false;
        }
        StrategyKey key = (StrategyKey) object;
        if (this.getId().equals(key.getId()) &&
                this.getRunner().equals(key.getRunner())) {
            return true;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id);
        result = prime * result + ((runner == null) ? 0 : runner.hashCode());
        return result;
    }
}
