import {text} from './data.mjs';
export function sqlName(value){text(value,'SQL identifier',200);if(!value.trim()||/[\u0000-\u001f\u007f]/.test(value))throw Error('Choose a nonempty identifier without control characters.');return '"'+value.replaceAll('"','""')+'"';}
export function migrationSQL(snapshot,operation){
  const q=sqlName,t=snapshot.tables.find(t=>t.name===operation.table);if(!t)throw Error('Select an existing table.');
  const col=()=>{if(!t.columns.some(c=>c.name===operation.column))throw Error('The source column no longer exists.');};
  const unused=name=>{q(name);if(t.columns.some(c=>c.name.toLowerCase()===name.toLowerCase()))throw Error('That column name is already present.');};
  switch(operation.kind){
    case 'rename-column':col();unused(operation.name);return `ALTER TABLE ${q(t.name)} RENAME COLUMN ${q(operation.column)} TO ${q(operation.name)};`;
    case 'rename-table':q(operation.name);if(snapshot.tables.some(t=>t.name.toLowerCase()===operation.name.toLowerCase()))throw Error('That table name is already present.');return `ALTER TABLE ${q(t.name)} RENAME TO ${q(operation.name)};`;
    case 'add-column':{
      unused(operation.name);if(!['TEXT','INTEGER','REAL','BLOB','NUMERIC'].includes(operation.type))throw Error('Choose a supported SQLite type.');
      let def='';if(operation.defaultValue!==null&&operation.defaultValue!==undefined){if(operation.type==='BLOB')throw Error('Binary defaults are not supported by this editor.');const value=String(operation.defaultValue);text(value,'Default',10000);def=" DEFAULT '"+value.replaceAll("'","''")+"'";}
      if(operation.notNull&&!def)throw Error('A required added column needs an explicit non-null default.');
      return `ALTER TABLE ${q(t.name)} ADD COLUMN ${q(operation.name)} ${operation.type}${operation.notNull?' NOT NULL':''}${def};`;
    }
    case 'create-index':col();q(operation.name);if(snapshot.indexNames.includes(operation.name))throw Error('That index already exists.');return `CREATE ${operation.unique?'UNIQUE ':''}INDEX ${q(operation.name)} ON ${q(t.name)} (${q(operation.column)});`;
    default:throw Error('Unsupported migration operation. No SQL was executed.');
  }
}
export const starterSQL=`CREATE TABLE organisations(id INTEGER PRIMARY KEY,name TEXT NOT NULL UNIQUE);
CREATE TABLE projects(id INTEGER PRIMARY KEY,organisation_id INTEGER NOT NULL REFERENCES organisations(id),title TEXT NOT NULL);
CREATE TABLE tasks(id INTEGER PRIMARY KEY,project_id INTEGER NOT NULL REFERENCES projects(id),title TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('open','done')));
CREATE INDEX tasks_project ON tasks(project_id);
INSERT INTO organisations VALUES(1,'Example workshop');
INSERT INTO projects VALUES(1,1,'Equipment maintenance'),(2,1,'Public demonstration');
INSERT INTO tasks VALUES(1,1,'Inspect equipment','open'),(2,1,'Record measurements','open'),(3,2,'Prepare demonstration','done');`;
