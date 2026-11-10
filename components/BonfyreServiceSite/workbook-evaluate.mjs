import { Univer, LocaleType, LifecycleService, LifecycleStages } from '@univerjs/core';
import { FUniver } from '@univerjs/core/facade';
import { UniverFormulaEnginePlugin } from '@univerjs/engine-formula';
import { UniverSheetsPlugin } from '@univerjs/sheets';
import sheetsEnUS from '@univerjs/sheets/locale/en-US';
import { UniverSheetsFormulaPlugin, CalculationMode } from '@univerjs/sheets-formula';
import '@univerjs/engine-formula/facade';
import '@univerjs/sheets/facade';
import '@univerjs/sheets-formula/facade';
import { validateWorkbook, calculationErrors, hasFormulas } from './workbook-model.mjs';
import {projectWorkbookDependencies} from './workbook-lineage.mjs';
import { setReviewCell } from './workbook-review.mjs';

export async function evaluateWorkbook({ snapshot, edits = [], review = null, includeLineage = false }) {
  let univer;
  try {
    validateWorkbook(snapshot);
    snapshot=structuredClone(snapshot);
    // A calculation result must replace every formula cache, including when a
    // browser worker has no rendering host to advance the plugin lifecycle.
    for(const id of snapshot.sheetOrder)for(const row of Object.values(snapshot.sheets[id].cellData??{}))for(const cell of Object.values(row))if(cell.f)delete cell.v;
    if (!hasFormulas(snapshot) && !edits.length) { return { snapshot, errors: calculationErrors(snapshot), engine: 'Univer 1.1.0', formulas: 0, ...(includeLineage?{lineage:projectWorkbookDependencies(snapshot,[])}:{}) }; }
    univer = new Univer({ locale: LocaleType.EN_US, locales: { enUS: sheetsEnUS } });
    univer.registerPlugin(UniverFormulaEnginePlugin);
    univer.registerPlugin(UniverSheetsPlugin);
    univer.registerPlugin(UniverSheetsFormulaPlugin, { initialFormulaComputing: CalculationMode.FORCED });
    const api = FUniver.newAPI(univer), workbook = api.createWorkbook(snapshot), formula = api.getFormula();
    // Univer 1.1.0 starts browser formula controllers at Rendered. This isolated
    // runtime has no UI renderer, so advance that documented lifecycle explicitly.
    univer.__getInjector().get(LifecycleService).stage=LifecycleStages.Rendered;
    for (const edit of edits) setReviewCell(workbook, review, edit.row, edit.role, edit.value);
    validateWorkbook(workbook.save());
    if (!hasFormulas(workbook.save())) { return { snapshot: workbook.save(), errors: calculationErrors(workbook.save()), engine: 'Univer 1.1.0', formulas: 0, ...(includeLineage?{lineage:projectWorkbookDependencies(workbook.save(),[])}:{}) }; }
    const done = formula.onCalculationResultApplied(8000);
    formula.executeCalculation();
    await done;
    const calculated = workbook.save(), errors = calculationErrors(calculated), circular = workbook.getCircularReferences();
    if (circular.length) throw Error('Circular references must be resolved before accepting this calculation.');
    const lineage=includeLineage?projectWorkbookDependencies(calculated,await formula.getAllDependencyTrees(8000)):null;
    return { snapshot: calculated, errors, engine: 'Univer 1.1.0',...(lineage?{lineage}:{}) };
  } finally { univer?.dispose(); }
}
