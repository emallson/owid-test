/**
 * Wrapper class for interacting with the records database.
 *
 * Since this is assumed to be an *internal* API (from the docs),
 * **sanitization is assumed already complete!**
 */
class RecordStore {
  constructor(pool, queue_size = 1000) {
    this.pool = pool;
    this._COUNTRIES = {};
    this.queue_size = 1000;
    this._queue = [];
  }

  async getVariables([country, year, ...vars]) {
    // the header row is always Country,Year,Var1,Var2,...
    this._VARIABLE_NAMES = vars;

    for(const name of vars) {
      await this.pool.query(
        "INSERT INTO variables (name) VALUES ($1) ON CONFLICT DO NOTHING;", 
        [name]
      );
    }

    const { rows } = await this.pool.query("SELECT * FROM variables WHERE name = ANY($1);", [vars]);

    const varMap = {};
    for(const { id, name } of rows) {
      varMap[name] = id;
    }

    return this._VARIABLE_NAMES.map(name => varMap[name]);
  }

  async getCountryId(country) {
    const { rows } = await this.pool.query("SELECT id FROM countries WHERE name = $1", [country]);
    if(rows.length !== 0) {
      return rows[0].id;
    }

    const { rows: insertedRows } = await this.pool.query("INSERT INTO countries (name) VALUES ($1) RETURNING id", [country]);
    return insertedRows[0].id;
  }

  async insert([country, year, ...vars], varIds) {
    const countryId = await this.getCountryId(country);
    for(let idx = 0; idx < vars.length; idx++) {
      const varId = varIds[idx];
      const value = vars[idx];
      if(value.length === 0) {
        continue; // won't insert empty value
      }
      const row = [countryId, year, varId, value];
      this._queue.push(row);
    }
  }

  isQueueFull() {
    return this._queue.length >= this.queue_size;
  }

  async processQueue(flush = false) {
    if(!flush && !this.isQueueFull()) {
      return;
    }
    const rows = this._queue;
    this._queue = [];

    const client = await this.pool.connect();
    await client.query('BEGIN');
    try {
      for(const row of rows) {
        await client.query("INSERT INTO records (countryId, year, varId, varValue) VALUES ($1, $2, $3, $4) ON CONFLICT (countryId, year, varId) DO UPDATE SET varValue = excluded.varValue;", row);
      }
    } catch(err) {
      console.err(err);
      await client.query('ROLLBACK');
    } finally {
      client.release();
    }
    await client.query('COMMIT');
  }

  async query(params) {
    const baseFields = {Country: 'countries.name', Year: 'year'};
    const variableField = (key, value) => `obj::json->>'${key}' = '${value}'`;

    const baseWhere = Object.entries(baseFields).filter(([key, _value]) => !!params[key])
      .map(([key, value]) => `${value} = '${params[key]}'`)
      .join(' AND ');
    // each row in the result has ONE variable value, so we OR these
    // together to collect results matching ANY of the variable values
    // listed
    const varWhere = Object.entries(params)
      .filter(([key, _value]) => !baseFields[key])
      .map((arr) => variableField(...arr))
      .join(' AND ');

    const innerQuery = `SELECT year, countries.name as country, json_object_agg(variables.name, varValue) as obj
                       FROM records
                       JOIN countries ON countries.id = countryId
                       JOIN variables ON variables.id = varId
                       ${(baseWhere.length > 0) ? `WHERE ${baseWhere}` : ""}
                       GROUP BY year, countries.name`;

    let query;
    if(varWhere.length > 0) {
      query = `SELECT year, country, obj FROM (${innerQuery}) as agg WHERE ${varWhere}`;
    } else {
      query = innerQuery;
    }

    const { rows } = await this.pool.query(query);
    return rows.map(({year, country, obj}) => Object.assign({year, country}, obj));
  }
}

module.exports = RecordStore;
