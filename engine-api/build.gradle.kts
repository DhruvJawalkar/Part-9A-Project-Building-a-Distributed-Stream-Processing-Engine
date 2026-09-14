// engine-api has ZERO dependencies, deliberately and permanently.
//
// This is the surface a job author writes against. Anything added here lands on
// their classpath, so the absence of a dependencies block below is the whole point
// of the file. If an API type ever seems to need a library, the type is wrong.
//
// (The shared JUnit/AssertJ test stack comes from the root build's subprojects
// block. Test-only libraries are not part of what this module exports.)
