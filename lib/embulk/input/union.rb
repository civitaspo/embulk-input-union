Embulk::JavaPlugin.register_input(
  "union", "org.embulk.input.union.UnionInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
