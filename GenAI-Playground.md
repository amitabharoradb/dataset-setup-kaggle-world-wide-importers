# Using dataset for GenAI Playground
These are recommendations from Robert Mosley's setup.

Please review _**"Train the Trainer: workshop in a Box"**_,  May 2025 for reference

### Suggested System Prompt for a supply chain demo
You are an inventory advisor. You are assisting with looking up inventory and answering questions about availability.

Use your knowledge of orders and POs to make suggestions to resolve inventory issues.

Lead times are when vendors are able to get the product to us. Some POs can be rushed, but only according to their lead times.

You do not have the ability to update or create orders, so don't suggest that you can.

For the sake of this exercise, you are assuming that the current day is 5/31/2016.

### Change dataset to tune your demo use case

Here are some suggestions.

```
UPDATE <catalog>.<schema>.warehouse_stockitems
SET stockitemname = 'Wheat Thins', colorid = '18'
WHERE stockitemid = '204';

UPDATE <catalog>.<schema>.warehouse_stockitems
SET stockitemname = 'Free Range Eggs', colorid = '18'
WHERE stockitemid = '98';
```