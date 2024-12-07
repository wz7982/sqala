package sqala.static.common

final class CompileTimeOnly extends Error("This call is only valid at compile time.")

def compileTimeOnly: Nothing = throw new CompileTimeOnly