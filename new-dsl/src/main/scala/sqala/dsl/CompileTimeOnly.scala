package sqala.dsl

final class CompileTimeOnly extends Error("This call is only valid at compile time.")

def compileTimeOnly: Nothing = throw new CompileTimeOnly