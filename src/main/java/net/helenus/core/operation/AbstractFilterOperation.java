/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package net.helenus.core.operation;

import java.util.*;
import net.helenus.core.*;
import net.helenus.core.cache.Facet;
import net.helenus.core.cache.UnboundFacet;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.HelenusProperty;

public abstract class AbstractFilterOperation<E, O extends AbstractFilterOperation<E, O>>
    extends AbstractOperation<E, O> {

  protected List<Filter<?>> filters = null;
  protected List<Filter<?>> ifFilters = null;

  public AbstractFilterOperation(AbstractSessionOperations sessionOperations) {
    super(sessionOperations);
  }

  public <V> O where(Getter<V> getter, Postulate<V> postulate) {

    addFilter(Filter.create(getter, postulate));

    return (O) this;
  }

  public <V> O where(Getter<V> getter, Operator operator, V val) {

    addFilter(Filter.create(getter, operator, val));

    return (O) this;
  }

  public <V> O where(Filter<V> filter) {

    addFilter(filter);

    return (O) this;
  }

  public <V> O and(Getter<V> getter, Postulate<V> postulate) {

    addFilter(Filter.create(getter, postulate));

    return (O) this;
  }

  public <V> O and(Getter<V> getter, Operator operator, V val) {

    addFilter(Filter.create(getter, operator, val));

    return (O) this;
  }

  public <V> O and(Filter<V> filter) {

    addFilter(filter);

    return (O) this;
  }

  public <V> O onlyIf(Getter<V> getter, Postulate<V> postulate) {

    addIfFilter(Filter.create(getter, postulate));

    return (O) this;
  }

  public <V> O onlyIf(Getter<V> getter, Operator operator, V val) {

    addIfFilter(Filter.create(getter, operator, val));

    return (O) this;
  }

  public <V> O onlyIf(Filter<V> filter) {

    addIfFilter(filter);

    return (O) this;
  }

  private void addFilter(Filter<?> filter) {
    if (filters == null) {
      filters = new LinkedList<Filter<?>>();
    }
    filters.add(filter);
  }

  private void addIfFilter(Filter<?> filter) {
    if (ifFilters == null) {
      ifFilters = new LinkedList<Filter<?>>();
    }
    ifFilters.add(filter);
  }

  @Override
  protected boolean isIdempotentOperation() {
    return filters
            .stream()
            .anyMatch(
                filter -> {
                  if (filter != null) {
                    HelenusPropertyNode node = filter.getNode();
                    if (node != null) {
                      HelenusProperty prop = node.getProperty();
                      if (prop != null) {
                        return prop.isIdempotent();
                      }
                    }
                  }
                  return false;
                })
        || super.isIdempotentOperation();
  }

  protected List<Facet> bindFacetValues(List<Facet> facets) {
    if (facets == null) {
      return new ArrayList<Facet>();
    }
    List<Facet> boundFacets = new ArrayList<>();
    Map<HelenusProperty, Filter> filterMap = new HashMap<>(filters.size());
    filters.forEach(f -> filterMap.put(f.getNode().getProperty(), f));

    for (Facet facet : facets) {
      if (facet instanceof UnboundFacet) {
        UnboundFacet unboundFacet = (UnboundFacet) facet;
        UnboundFacet.Binder binder = unboundFacet.binder();
        if (filters != null) {
          for (HelenusProperty prop : unboundFacet.getProperties()) {

            Filter filter = filterMap.get(prop);
            if (filter != null) {
              Object[] postulates = filter.postulateValues();
              for (Object p : postulates) {
                binder.setValueForProperty(prop, p.toString());
              }
            }
          }
        }
        if (binder.isBound()) {
          boundFacets.add(binder.bind());
        }
      } else {
        boundFacets.add(facet);
      }
    }
    return boundFacets;
  }
}
