module.exports.handler = async (event, context) => {
  const mod = await import("./app.mjs");
  return mod.handler(event, context);
};