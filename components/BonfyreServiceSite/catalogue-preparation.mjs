// Preparation proposals start empty. They are not values observed at the merchant.
export const preparationFields = [
  { field: 'prep:seo_title', label: 'Proposed search title', max: 255 },
  { field: 'prep:seo_description', label: 'Proposed search description', max: 2000 },
  { field: 'prep:image_filename', label: 'Proposed image filename', max: 255 },
  { field: 'prep:image_alt', label: 'Proposed image alt text', max: 2000 },
  { field: 'prep:collections', label: 'Proposed collections', max: 4000 },
  { field: 'prep:questions', label: 'Questions for approval', max: 6000 }
];
export const emptyPreparation = () => Object.fromEntries(preparationFields.map(({ field }) => [field, '']));
