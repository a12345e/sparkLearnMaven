/**
 * The domain model.
 *
 * <p>Empty by design, for now. The model is grown <em>test-first</em>: each
 * element is specified by a test in
 * {@code src/test/java/com/example/ontologyprocessor/transform/model/} against
 * small in-memory fixtures, and only the code needed to satisfy that test lands
 * here.
 *
 * <p>It sits in the transform stage because that is where domain shaping
 * happens: extract knows about sources, load knows about sinks, and neither
 * should know about the model. A model element is normally a
 * {@link com.example.ontologyprocessor.transform.Transformer}.
 *
 * <p>There is deliberately no application wired around the model — no job reads
 * production data through it. Tests are the only caller, so the model's shape
 * stays driven by its specification rather than by an integration.
 *
 * <p>See {@code ModelDevelopmentExampleTest} for the working loop.
 */
package com.example.ontologyprocessor.transform.model;
