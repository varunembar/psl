/*
 * This file is part of the PSL software.
 * Copyright 2011-2015 University of Maryland
 * Copyright 2013-2017 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.linqs.psl.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.linqs.psl.PSLTest;
import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.config.EmptyBundle;
import org.linqs.psl.database.DataStore;
import org.linqs.psl.database.rdbms.RDBMSDataStore;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import org.linqs.psl.model.predicate.PredicateFactory;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.model.term.ConstantType;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ModelLoaderTest {
	private DataStore dataStore;

	private StandardPredicate singlePredicate;
	private StandardPredicate doublePredicate;

	@Before
	public void setup() {
		ConfigBundle config = new EmptyBundle();
		dataStore = new RDBMSDataStore(new H2DatabaseDriver(Type.Memory, this.getClass().getName(), true), config);

		PredicateFactory factory = PredicateFactory.getFactory();

		singlePredicate = factory.createStandardPredicate("Single", ConstantType.UniqueID);
		dataStore.registerPredicate(singlePredicate);

		doublePredicate = factory.createStandardPredicate("Double", ConstantType.UniqueID, ConstantType.UniqueID);
		dataStore.registerPredicate(doublePredicate);
	}

	@Test
	public void testBase() {
		String input =
			"1: Single(A) & Double(A, B) >> Single(B) ^2\n" +
			"5: Single(B) & Double(B, A) >> Single(A) ^2\n";
		String[] expected = new String[]{
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"5.0: ( SINGLE(B) & DOUBLE(B, A) ) >> SINGLE(A) ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	// Shortest rule we can think of.
	public void testShortRule() {
		String input =
			"~Single(A) .";
		String[] expected = new String[]{
			"~( SINGLE(A) ) ."
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	// Very long rule.
	public void testLongRule() {
		List<String> parts = new ArrayList<String>();
		for (char i = 'A'; i < 'Z'; i++) {
			parts.add(String.format("Single(%c) & Double(%c, Z)", i, i));
		}
		String input = String.format("1: %s >> Single(Z) ^2", StringUtils.join(parts, " & "));
		String expected = String.format("1.0: ( %s ) >> SINGLE(Z) ^2", StringUtils.join(parts, " & ").toUpperCase());

		PSLTest.assertModel(dataStore, input, new String[]{expected});
	}

	@Test
	// General check for comment support.
	public void testComments() {
		String input =
			"# This is a comment!\n" +
			"#This is a comment!\n" +
			"## This is another comment (but actually the same form).\n" +
			"		# This is a comment!\n" +
			"\n" +
			"//This is a comment!\n" +
			"// This is a comment!\n" +
			"//// This is another comment (but actually the same form).\n" +
			"		// This is a comment!\n" +
			"\n" +
			"/* Block time! */\n" +
			"/* Block time!\n" +
			" Sill in a comment\n" +
			"*/\n" +
			"/** Block time (javadoc style)!\n" +
			" * Sill in a comment\n" +
			" */\n" +
			"\n" +
			"1: Single(A) & Double(A, B) >> Single(B) ^2\n" +
			"// Another comment.\n" +
			"1: Single(A) & Double(A, B) >> Single(B) ^2 // Inline comment!\n" +
			"1: Single(A) & Double(A, B) >> Single(B) ^2 # Inline comment!\n" +
			"1: Single(A) & Double(A, B) >> Single(B) ^2 /* Inline comment! */\n" +
			"1: Single(A) & Double(A, B) >> Single(B) // ^2 // Changing a rule.\n" +
			"1: Single(A) & Double(A, B) /* & Single(C) */ >> Single(B) ^2 // Inside of other syntax.\n" +
			"";
		String[] expected = new String[]{
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B)",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testStringConstants() {
		String input =
			"1: Single(A) & Double(A, \"bar\") & Single(\"bar\") >> Double(A, \"bar\") ^2\n" +
			"1: Single(A) & Double(A, 'bar') & Single('bar') >> Double(A, 'bar') ^2\n";
		String[] expected = new String[]{
			"1.0: ( SINGLE(A) & DOUBLE(A, 'bar') & SINGLE('bar') ) >> DOUBLE(A, 'bar') ^2",
			"1.0: ( SINGLE(A) & DOUBLE(A, 'bar') & SINGLE('bar') ) >> DOUBLE(A, 'bar') ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	// We are actually testing both numeric constants and coefficients.
	public void testNumericConstants() {
		/* TODO(eriq): Awaiting word from Steve/Jay about numeric constants being allowed?
			"1: Single(A) & Double(A, 1) >> Single(B) ^2\n" +
		*/
		String input =
			"1: 1 Single(A) = 1 ^2\n" +
			"1: 1.0 Single(A) = 1 ^2\n" +
			"1: 1.5 Single(A) = 1 ^2\n" +
			"1: 0.5 Single(A) = 1 ^2\n" +
			"1: -1.0 Single(A) = 1 ^2\n" +
			"1: 5E10 Single(A) = 1 ^2\n" +
			"1: 5e10 Single(A) = 1 ^2\n" +
			"1: -5e10 Single(A) = 1 ^2\n" +
			"1: 5e-10 Single(A) = 1 ^2\n" +
			"1: 1.2e10 Single(A) = 1 ^2\n" +
			"1: -1.2e10 Single(A) = 1 ^2\n" +
			"1: 1.2e-10 Single(A) = 1 ^2\n" +
			"";
		String[] expected = new String[]{
			"1.0: 1.0 * SINGLE(A) = 1.0 ^2",
			"1.0: 1.0 * SINGLE(A) = 1.0 ^2",
			"1.0: 1.5 * SINGLE(A) = 1.0 ^2",
			"1.0: 0.5 * SINGLE(A) = 1.0 ^2",
			"1.0: -1.0 * SINGLE(A) = 1.0 ^2",
			"1.0: 5.0E10 * SINGLE(A) = 1.0 ^2",
			"1.0: 5.0E10 * SINGLE(A) = 1.0 ^2",
			"1.0: -5.0E10 * SINGLE(A) = 1.0 ^2",
			"1.0: 5.0E-10 * SINGLE(A) = 1.0 ^2",
			"1.0: 1.2E10 * SINGLE(A) = 1.0 ^2",
			"1.0: -1.2E10 * SINGLE(A) = 1.0 ^2",
			"1.0: 1.2E-10 * SINGLE(A) = 1.0 ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	// Using some more unusual identifiers.
	public void testIdentifiers() {
		String input =
			"1: Single(A1) >> Single(A1) ^2\n" +
			"1: Single(A1A) >> Single(A1A) ^2\n" +
			"1: Single(A_A) >> Single(A_A) ^2\n" +
			"1: Single(A_1) >> Single(A_1) ^2\n" +
			"1: Single(A__) >> Single(A__) ^2\n" +
			"";
		String[] expected = new String[]{
			"1.0: SINGLE(A1) >> SINGLE(A1) ^2",
			"1.0: SINGLE(A1A) >> SINGLE(A1A) ^2",
			"1.0: SINGLE(A_A) >> SINGLE(A_A) ^2",
			"1.0: SINGLE(A_1) >> SINGLE(A_1) ^2",
			"1.0: SINGLE(A__) >> SINGLE(A__) ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	// Test possible values for the rule weight.
	public void testWeight() {
		String input =
			"1: Single(A) >> Single(A)\n" +
			"0: Single(A) >> Single(A)\n" +
			"0.5: Single(A) >> Single(A)\n" +
			"999999: Single(A) >> Single(A)\n" +
			"9999999999: Single(A) >> Single(A)\n" +
			"0000000001: Single(A) >> Single(A)\n" +
			"0.001: Single(A) >> Single(A)\n" +
			"0.00000001: Single(A) >> Single(A)\n" +
			"2E10: Single(A) >> Single(A)\n" +
			"2e10: Single(A) >> Single(A)\n" +
			"2e-10: Single(A) >> Single(A)\n" +
			"2.5e10: Single(A) >> Single(A)\n" +
			"2.5e-10: Single(A) >> Single(A)\n" +
			"";
		String[] expected = new String[]{
			"1.0: SINGLE(A) >> SINGLE(A)",
			"0.0: SINGLE(A) >> SINGLE(A)",
			"0.5: SINGLE(A) >> SINGLE(A)",
			"999999.0: SINGLE(A) >> SINGLE(A)",
			"9.999999999E9: SINGLE(A) >> SINGLE(A)",
			"1.0: SINGLE(A) >> SINGLE(A)",
			"0.001: SINGLE(A) >> SINGLE(A)",
			"1.0E-8: SINGLE(A) >> SINGLE(A)",
			"2.0E10: SINGLE(A) >> SINGLE(A)",
			"2.0E10: SINGLE(A) >> SINGLE(A)",
			"2.0E-10: SINGLE(A) >> SINGLE(A)",
			"2.5E10: SINGLE(A) >> SINGLE(A)",
			"2.5E-10: SINGLE(A) >> SINGLE(A)"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testImpliedBy() {
		String input =
			"1: Single(A) & Double(A, B) >> Single(B) ^2\n" +
			"1: Single(B) << Single(A) & Double(A, B) ^2\n";
		String[] expected = new String[]{
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testDisjunction() {
		String input =
			"1: Single(A) | Double(A, B) << Single(B) & Single(A) ^2\n" +
			"1: Single(A) & Double(B, C) >> Single(B) | Single(C) ^2\n";
		String[] expected = new String[]{
			"1.0: ( SINGLE(B) & SINGLE(A) ) >> ( SINGLE(A) | DOUBLE(A, B) ) ^2",
			"1.0: ( SINGLE(A) & DOUBLE(B, C) ) >> ( SINGLE(B) | SINGLE(C) ) ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	// You cannot negate any expression because we only allow conjunctions in the body and disjunctions in the head.
	public void testNegation() {
		String input =
			"1: ~Single(A) & Double(A, B) >> ~Single(B) ^2\n";
		String[] expected = new String[]{
			"1.0: ( ~( SINGLE(A) ) & DOUBLE(A, B) ) >> ~( SINGLE(B) ) ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);

		try {
			PSLTest.assertRule(dataStore, "1: ~Single(A) & ~~Double(A, B) >> ~~~Single(B) ^2", "");
			fail("Negation allowed on non-atom (negation).");
		} catch (org.antlr.v4.runtime.NoViableAltException ex) {
			// Exception expected.
		}

		try {
			PSLTest.assertRule(dataStore, "1: ~( Single(A) & Single(B) ) >> Double(A, B) ^2", "");
			fail("Negation allowed on non-atom (conjunction).");
		} catch (org.antlr.v4.runtime.NoViableAltException ex) {
			// Exception expected.
		}

		try {
			PSLTest.assertRule(dataStore, "1: Double(A, B) >> ~( Single(A) | Single(B) ) ^2", "");
			fail("Negation allowed on non-atom (disjunction).");
		} catch (org.antlr.v4.runtime.NoViableAltException ex) {
			// Exception expected.
		}
	}

	@Test
	public void testTermEquality() {
		String input =
			"1: A == B & Double(A, B) >> Single(B) ^2\n" +
			"1: A == 'Bar' & Double(A, B) >> Single(B) ^2\n" +
			"1: 'Foo' == B & Double(A, B) >> Single(B) ^2\n" +
			"1: 'Foo' == 'Bar' & Double(A, B) >> Single(B) ^2\n" +
			"1: A ~= B & Double(A, B) >> Single(B) ^2\n" +
			"1: A ~= 'Bar' & Double(A, B) >> Single(B) ^2\n" +
			"1: 'Foo' ~= B & Double(A, B) >> Single(B) ^2\n" +
			"1: 'Foo' ~= 'Bar' & Double(A, B) >> Single(B) ^2\n" +
			"";
		String[] expected = new String[]{
			"1.0: ( (A == B) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( (A == 'Bar') & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( ('Foo' == B) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( ('Foo' == 'Bar') & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( (A != B) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( (A != 'Bar') & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( ('Foo' != B) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( ('Foo' != 'Bar') & DOUBLE(A, B) ) >> SINGLE(B) ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testAlternativeSyntax() {
		String input =
			"1: Single(A) & Double(A, B) >> Single(B)\n" +
			"1: Single(A) && Double(A, B) >> Single(B)\n" +
			"1: Single(A) & Double(A, B) >> Single(B)\n" +
			"1: Single(A) & Double(A, B) -> Single(B)\n" +
			"1: Single(A) | Single(B) << Double(A, B)\n" +
			"1: Single(A) || Single(B) << Double(A, B)\n" +
			"1: Single(A) | Single(B) << Double(A, B)\n" +
			"1: Single(A) | Single(B) <- Double(A, B)\n" +
			"1: A != B & Double(A, B) >> Single(B)\n" +
			"1: A ~= B & Double(A, B) >> Single(B)\n" +
			"";
		String[] expected = new String[]{
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B)",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B)",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B)",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B)",
			"1.0: DOUBLE(A, B) >> ( SINGLE(A) | SINGLE(B) )",
			"1.0: DOUBLE(A, B) >> ( SINGLE(A) | SINGLE(B) )",
			"1.0: DOUBLE(A, B) >> ( SINGLE(A) | SINGLE(B) )",
			"1.0: DOUBLE(A, B) >> ( SINGLE(A) | SINGLE(B) )",
			"1.0: ( (A != B) & DOUBLE(A, B) ) >> SINGLE(B)",
			"1.0: ( (A != B) & DOUBLE(A, B) ) >> SINGLE(B)"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testMultiplyCoefficient() {
		String input =
			"1: 1 Single(A) = 1 ^2\n" +
			"1: 1 * Single(A) = 1 ^2\n" +
			"";
		String[] expected = new String[]{
			"1.0: 1.0 * SINGLE(A) = 1.0 ^2",
			"1.0: 1.0 * SINGLE(A) = 1.0 ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	// Just throw in a bunch of arithmetic rules used in other tests.
	public void testGeneralArithmetic() {
		String input =
			"Single(A) + Single(B) = 1 .\n" +
			"Double(+A, 'Foo') = 1 .\n" +
			"Single(+A) + Single(+B) = 1 .\n" +
			"Single(+A) = 1 . {A: Single(A)}\n" +
			"Single(+A) = 1 . {A: Single(A) || Double(A, A) }\n" +
			"Double(+A, B) = 1 . {A: Single(B)}\n" +
			"Single(+A) + Single(+B) = 1 . {A: Single(A)} {B: Single(B)}\n" +
			"|A| Single(+A) = 1 .\n" +
			"|A| Single(+A) = |A| .\n" +
			"|A| Single(+A) + |B| Single(+B) = 1 .\n" +
			"@Max[|A|, 0] Single(+A) = 1 .\n" +
			"@Max[1, 0] Single(+A) = 1 .\n" +
			"@Max[|A|, |B|] Single(+A) + Single(+B) = 1 .\n" +
			"@Min[1, 0] Single(A) = 1 .\n" +
			"";
		String[] expected = new String[]{
			"1.0 * SINGLE(A) + 1.0 * SINGLE(B) = 1.0 .",
			"1.0 * DOUBLE(+A, 'Foo') = 1.0 .",
			"1.0 * SINGLE(+A) + 1.0 * SINGLE(+B) = 1.0 .",
			"1.0 * SINGLE(+A) = 1.0 .\n{A : SINGLE(A)}",
			"1.0 * SINGLE(+A) = 1.0 .\n{A : ( SINGLE(A) | DOUBLE(A, A) )}",
			"1.0 * DOUBLE(+A, B) = 1.0 .\n{A : SINGLE(B)}",
			"1.0 * SINGLE(+A) + 1.0 * SINGLE(+B) = 1.0 .\n{A : SINGLE(A)}\n{B : SINGLE(B)}",
			"|A| * SINGLE(+A) = 1.0 .",
			"|A| * SINGLE(+A) = |A| .",
			"|A| * SINGLE(+A) + |B| * SINGLE(+B) = 1.0 .",
			"@Max[|A|, 0.0] * SINGLE(+A) = 1.0 .",
			"1.0 * SINGLE(+A) = 1.0 .",
			"@Max[|A|, |B|] * SINGLE(+A) + 1.0 * SINGLE(+B) = 1.0 .",
			"0.0 * SINGLE(A) = 1.0 ."
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testLoadRuleBase() {
		String input = "1: Single(A) & Double(A, B) >> Single(B) ^2";
		String expected = "1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2";
		PSLTest.assertRule(dataStore, input, expected);
	}

	@Test
	public void testLoadRuleBadCount() {
		// Having zero rules is a parse error, so the exception is different.
		try {
			PSLTest.assertRule(dataStore, "// Just a comment", "");
			fail("ModelLoader.LoadRule() with no rule did not throw an exception.");
		} catch (org.antlr.v4.runtime.NoViableAltException ex) {
			// Exception expected.
		}

		String input =
			"1: Single(A) & Double(A, B) >> Single(B) ^2\n" +
			"5: Single(B) & Double(B, A) >> Single(A) ^2\n";
		String expected = "1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2";

		try {
			PSLTest.assertRule(dataStore, input, expected);
			fail("ModelLoader.LoadRule() with more than one rule did not throw an exception.");
		} catch (IllegalArgumentException ex) {
			// Exception expected.
		}
	}

	@Test
	// Floats must have a leading number (".1" is not good, must be "0.1").
	public void testLeadDigitOnFloat() {
		String[] input = new String[]{
			".1: Single(A) & Double(A, B) >> Single(B) ^2",
			"1: .1 Single(A) = 1 ^2"
		};
		String[] expected = new String[]{
			"0.1: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: 0.1 * SINGLE(A) = 1.0 ^2"
		};

		for (int i = 0; i < input.length; i++) {
			try {
				PSLTest.assertRule(dataStore, input[i], expected[i]);
				fail(String.format("Rule: %d - Exception not thrown when float used without leading digit.", i));
			} catch (Exception ex) {
				// Exception expected.
			}
		}
	}

	@Test
	// Various misc syntax errors.
	public void testGeneralBadSyntax() {
		String[] input = new String[]{
			// Missing comma
			"1: Single(A) & Double(A B) >> Single(B) ^2",
			// Unknown predicate
			"1: Unknown(A) & Double(A, B) >> Single(B) ^2",
			// Mismatched quotes.
			"1: Single(A) & Double(\"Foo', B) >> Single(B) ^2",
			// Mismatched parens.
			"1: ( Single(A) & Double(A, B) >> Single(B) ^2",
			"1: Single(A) & Double(A, B) ) >> Single(B) ^2",
			// Missing unweighted period.
			"Single(A) & Double(A, B) >> Single(B) ^2",
			// Negative weight
			"-1: Single(A) & Double(A B) >> Single(B) ^2"
		};
		String[] expected = new String[]{
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( UNKNOWN(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( SINGLE(A) & DOUBLE('Foo', B) ) >> SINGLE(B) ^2",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) . ^2",
			"-1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2"
		};

		for (int i = 0; i < input.length; i++) {
			try {
				PSLTest.assertRule(dataStore, input[i], expected[i]);
				fail(String.format("Rule: %d - Exception not thrown on general syntax error.", i));
			} catch (Exception ex) {
				// Exception expected.
			}
		}
	}

	@Test
	public void testBadSquaring() {
		String[] input = new String[]{
			// TODO(eriq): This is a bad input but not caught by the parser.
			// "1: Single(A) & Double(A, B) >> Single(B) ^2.5",
			"1: Single(A) & Double(A, B) >> Single(B) ^3",
			"1: Single(A) & Double(A, B) >> Single(B) ^-1",
			"1: Single(A) & Double(A, B) >> Single(B) ^-2.0",
			"1: Single(A) & Double(A, B) >> Single(B) ^-2.5",
			"1: Single(A) & Double(A, B) >> Single(B) ^-3"
		};
		String[] expected = new String[]{
			// "1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B)",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B)",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B)",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B)",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B)",
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B)"
		};

		for (int i = 0; i < input.length; i++) {
			try {
				PSLTest.assertRule(dataStore, input[i], expected[i]);
				fail(String.format("Rule: %d - Exception not thrown on bad square error.", i));
			} catch (Exception ex) {
				// Exception expected.
			}
		}
	}

	@Test
	// First test only rules that are fully specified.
	public void testLoadRulePartialCompleteRules() {
		String[] inputs = new String[]{
			"1: Single(A) & Double(A, B) >> Single(B) ^2",
			"Single(A) & Double(A, B) >> Single(B) .",
			"1: 1 Single(A) = 1 ^2",
			"1 Single(A) = 1 .",
			"Single(+A) = 1 . {A: Single(A)}",
			"1: Single(+A) = 1 {A: Single(A)}",
			"1: Single(+A) = 1 ^2 {A: Single(A)}"
		};

		String[] expected = new String[]{
			"1.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) .",
			"1.0: 1.0 * SINGLE(A) = 1.0 ^2",
			"1.0 * SINGLE(A) = 1.0 .",
			"1.0 * SINGLE(+A) = 1.0 .\n{A : SINGLE(A)}",
			"1.0: 1.0 * SINGLE(+A) = 1.0\n{A : SINGLE(A)}",
			"1.0: 1.0 * SINGLE(+A) = 1.0 ^2\n{A : SINGLE(A)}",
		};

		try {
			for (int i = 0; i < inputs.length; i++) {
				RulePartial partial = ModelLoader.loadRulePartial(dataStore, inputs[i]);
				assertEquals(
						String.format("Expected RulePartial #%d to be a rule, but was not.", i),
						true,
						partial.isRule()
				);

				Rule rule = partial.toRule();
				PSLTest.assertStringEquals(expected[i], rule.toString(), true,
						String.format("Rule %d string mismatch", i));
			}
		} catch (IOException ex) {
			fail("Unexpected IOException thrown from ModelLoader.loadRulePartial(): " + ex);
		}
	}

	@Test
	// First test only rules that are fully specified.
	public void testLoadRulePartialPartialRules() {
		String[] inputs = new String[]{
			"Single(A) & Double(A, B) >> Single(B)",
			"1 Single(A) = 1",
			"Single(+A) = 1 {A: Single(A)}",
			"Single(+A) + Single(+B) = 1 {A: Single(A)} {B: Single(B)}"
		};

		String[] unweightedExpected = new String[]{
			"( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) .",
			"1.0 * SINGLE(A) = 1.0 .",
			"1.0 * SINGLE(+A) = 1.0 .\n{A : SINGLE(A)}",
			"1.0 * SINGLE(+A) + 1.0 * SINGLE(+B) = 1.0 .\n{A : SINGLE(A)}\n{B : SINGLE(B)}"
		};

		// Weight all the variants with 5 and square them.
		String[] weightedExpected = new String[]{
			"5.0: ( SINGLE(A) & DOUBLE(A, B) ) >> SINGLE(B) ^2",
			"5.0: 1.0 * SINGLE(A) = 1.0 ^2",
			"5.0: 1.0 * SINGLE(+A) = 1.0 ^2\n{A : SINGLE(A)}",
			"5.0: 1.0 * SINGLE(+A) + 1.0 * SINGLE(+B) = 1.0 ^2\n{A : SINGLE(A)}\n{B : SINGLE(B)}"
		};

		try {
			for (int i = 0; i < inputs.length; i++) {
				RulePartial partial = ModelLoader.loadRulePartial(dataStore, inputs[i]);
				assertEquals(
						String.format("Expected RulePartial #%d to not a rule, but was.", i),
						false,
						partial.isRule()
				);

				Rule unweightedRule = partial.toRule();
				PSLTest.assertStringEquals(unweightedExpected[i], unweightedRule.toString(), true,
						String.format("Unweighted rule %d string mismatch", i));

				Rule weightedRule = partial.toRule(5.0, true);
				PSLTest.assertStringEquals(weightedExpected[i], weightedRule.toString(), true,
						String.format("Weighted rule %d string mismatch", i));
			}
		} catch (IOException ex) {
			fail("Unexpected IOException thrown from ModelLoader.loadRulePartial(): " + ex);
		}
	}

	@Test
	public void testNonSymmetric() {
		String input =
			"1: Single(A) & Single(B) & (A % B) >> Double(A, B) ^2\n" +
			"1: Single(A) & Single(B) & (A ^ B) >> Double(A, B) ^2\n" +
			"";
		String[] expected = new String[]{
			"1.0: ( SINGLE(A) & SINGLE(B) & (A % B) ) >> DOUBLE(A, B) ^2",
			"1.0: ( SINGLE(A) & SINGLE(B) & (A % B) ) >> DOUBLE(A, B) ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testArithmeticCoefficientOperationOrder() {
		String input =
			"1.0 + 2.0 * 3.0 * Single(A) = 99 .\n" +
			"1.0 + 2.0 * 3.0 * Single(A) + Single(B) = 99 .\n" +
			"99 = 1.0 + 2.0 * 3.0 * Single(A) .\n" +
			"1.0 + 2.0 * 3.0 * Single(A) = 4.0 + 5.0 * 6.0 * Single(B) .\n" +
			"1.0 + 2.0 - 3.0 * Single(A) = 99 .\n" +
			"1.0 - 2.0 + 3.0 * Single(A) = 99 .\n" +
			"1.0 + 2.0 + 3.0 - 4.0 * Single(A) = 99 .\n" +
			"1.0 - 2.0 - 3.0 + 4.0 * Single(A) = 99 .\n" +
			"1.0 + (2.0 * 3.0) * Single(A) = 99 .\n" +
			"(1.0 + 2.0) * 3.0 * Single(A) = 99 .\n" +
			"1.0 + 2.0 * |A| * Single(+A) = 99 .\n" +
			"1.0 + 2.0 * @Min(3.0, 4.0) * Single(+A) = 99 .\n" +
			"1.0 + 2.0 * @Max(3.0, 4.0) * Single(+A) = 99 .\n" +
			"1.0 + 2.0 * 3.0 + 4.0 * Single(A) = 99 .\n" +
			"1.0 + (2.0 * 3.0) + 4.0 * Single(A) = 99 .\n" +
			"(1.0 + 2.0) * (3.0 + 4.0) * Single(A) = 99 .\n" +
			"1.0 - 2.0 / 4.0 - 5.0 * Single(A) = 99 .\n" +
			"1.0 - (2.0 / 4.0) - 5.0 * Single(A) = 99 .\n" +
			"(1.0 - 2.0) / (4.0 - 5.0) * Single(A) = 99 .\n" +
			"";
		String[] expected = new String[]{
			"7.0 * SINGLE(A) = 99.0 .",
			"7.0 * SINGLE(A) + 1.0 * SINGLE(B) = 99.0 .",
			"-7.0 * SINGLE(A) = -99.0 .",
			"7.0 * SINGLE(A) + -34.0 * SINGLE(B) = 0.0 .",
			"0.0 * SINGLE(A) = 99.0 .",
			"2.0 * SINGLE(A) = 99.0 .",
			"2.0 * SINGLE(A) = 99.0 .",
			"0.0 * SINGLE(A) = 99.0 .",
			"7.0 * SINGLE(A) = 99.0 .",
			"9.0 * SINGLE(A) = 99.0 .",
			"(1.0 + (2.0 * |A|)) * SINGLE(+A) = 99.0 .",
			"7.0 * SINGLE(+A) = 99.0 .",
			"9.0 * SINGLE(+A) = 99.0 .",
			"11.0 * SINGLE(A) = 99.0 .",
			"11.0 * SINGLE(A) = 99.0 .",
			"21.0 * SINGLE(A) = 99.0 .",
			"-4.5 * SINGLE(A) = 99.0 .",
			"-4.5 * SINGLE(A) = 99.0 .",
			"1.0 * SINGLE(A) = 99.0 ."
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testNotEquals() {
		// Test both syntaxes.
		String input =
			"1: A != B & Single(A) & Single(B) >> Double(A, B) ^2\n" +
			"1: (A != B) & Single(A) & Single(B) >> Double(A, B) ^2\n" +
			"1: A!=B & Single(A) & Single(B) >> Double(A, B) ^2\n" +
			"1: (A!=B) & Single(A) & Single(B) >> Double(A, B) ^2\n" +
			"1: A - B & Single(A) & Single(B) >> Double(A, B) ^2\n" +
			"1: (A - B) & Single(A) & Single(B) >> Double(A, B) ^2\n" +
			"1: A-B & Single(A) & Single(B) >> Double(A, B) ^2\n" +
			"1: (A-B) & Single(A) & Single(B) >> Double(A, B) ^2\n" +
			"";
		String[] expected = new String[]{
			"1.0: ( (A != B) & SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B) ^2",
			"1.0: ( (A != B) & SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B) ^2",
			"1.0: ( (A != B) & SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B) ^2",
			"1.0: ( (A != B) & SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B) ^2",
			"1.0: ( (A != B) & SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B) ^2",
			"1.0: ( (A != B) & SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B) ^2",
			"1.0: ( (A != B) & SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B) ^2",
			"1.0: ( (A != B) & SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B) ^2"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testFilterOperatorOrder() {
		String input =
			"Single(+A) + Double(B, C) = 1 . {A: Single(A) || Single(B) || Single(C)}\n" +
			"Single(+A) + Double(B, C) = 1 . {A: Single(A) && Single(B) && Single(C)}\n" +
			"Single(+A) + Double(B, C) = 1 . {A: Single(A) || Single(B) && Single(C)}\n" +
			"Single(+A) + Double(B, C) = 1 . {A: Single(A) && Single(B) || Single(C)}\n" +

			"Single(+A) + Double(B, C) = 1 . {A: (Single(A) || Single(B)) || Single(C)}\n" +
			"Single(+A) + Double(B, C) = 1 . {A: Single(A) || (Single(B) || Single(C))}\n" +
			"Single(+A) + Double(B, C) = 1 . {A: (Single(A) && Single(B)) && Single(C)}\n" +
			"Single(+A) + Double(B, C) = 1 . {A: Single(A) && (Single(B) && Single(C))}\n" +

			"Single(+A) + Double(B, C) = 1 . {A: (Single(A) || Single(B)) && Single(C)}\n" +
			"Single(+A) + Double(B, C) = 1 . {A: Single(A) || (Single(B) && Single(C))}\n" +
			"Single(+A) + Double(B, C) = 1 . {A: (Single(A) && Single(B)) || Single(C)}\n" +
			"Single(+A) + Double(B, C) = 1 . {A: Single(A) && (Single(B) || Single(C))}\n" +
			"";
		String[] expected = new String[]{
			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( SINGLE(A) | SINGLE(B) | SINGLE(C) )}",
			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( SINGLE(A) & SINGLE(B) & SINGLE(C) )}",
			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( SINGLE(A) | ( SINGLE(B) & SINGLE(C) ) )}",
			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( ( SINGLE(A) & SINGLE(B) ) | SINGLE(C) )}",

			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( SINGLE(A) | SINGLE(B) | SINGLE(C) )}",
			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( SINGLE(A) | SINGLE(B) | SINGLE(C) )}",
			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( SINGLE(A) & SINGLE(B) & SINGLE(C) )}",
			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( SINGLE(A) & SINGLE(B) & SINGLE(C) )}",

			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( ( SINGLE(A) & SINGLE(C) ) | ( SINGLE(B) & SINGLE(C) ) )}",
			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( SINGLE(A) | ( SINGLE(B) & SINGLE(C) ) )}",
			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( ( SINGLE(A) & SINGLE(B) ) | SINGLE(C) )}",
			"1.0 * SINGLE(+A) + 1.0 * DOUBLE(B, C) = 1.0 .\n{A : ( ( SINGLE(A) & SINGLE(B) ) | ( SINGLE(A) & SINGLE(C) ) )}"
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testArithmeticSubtraction() {
		String input =
			"Double(A, B) - Double(B, A) = 0.0 .\n" +
			"0.0 = Double(A, B) - Double(B, A) .\n" +
			"Double(A, B) + Double(B, A) = 0.0 .\n" +
			"0.0 = Double(A, B) + Double(B, A) .\n" +
			"";
		String[] expected = new String[]{
			"1.0 * DOUBLE(A, B) + -1.0 * DOUBLE(B, A) = 0.0 .",
			"-1.0 * DOUBLE(A, B) + 1.0 * DOUBLE(B, A) = 0.0 .",
			"1.0 * DOUBLE(A, B) + 1.0 * DOUBLE(B, A) = 0.0 .",
			"-1.0 * DOUBLE(A, B) + -1.0 * DOUBLE(B, A) = 0.0 ."
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testLogicalParens() {
		String input =
			"1.0: Single(A) & Single(B) >> Double(A, B) ^2\n" +
			"1.0: Single(A) & Single(B) >> Double(A, B)\n" +
			"1.0: ( Single(A) && Single(B) ) -> Double(A, B) ^2\n" +
			"1.0: ( Single(A) && Single(B) ) -> Double(A, B)\n" +

			"1.0: (Single(A)) & Single(B) >> Double(A, B)\n" +
			"1.0: (Single(A)) & (Single(B)) >> Double(A, B)\n" +
			"1.0: ((Single(A)) & (Single(B))) >> Double(A, B)\n" +
			"1.0: (Single(A)) & Single(B) >> (Double(A, B))\n" +
			"1.0: (((Single(A)) & (Single(B))) & Double(B, A)) >> Double(A, B)\n" +
			"";
		String[] expected = new String[]{
			"1.0: ( SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B) ^2",
			"1.0: ( SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B)",
			"1.0: ( SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B) ^2",
			"1.0: ( SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B)",

			"1.0: ( SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B)",
			"1.0: ( SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B)",
			"1.0: ( SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B)",
			"1.0: ( SINGLE(A) & SINGLE(B) ) >> DOUBLE(A, B)",
			"1.0: ( SINGLE(A) & SINGLE(B) & DOUBLE(B, A) ) >> DOUBLE(A, B)",
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testArithmeticParens() {
		String input =
			"Single(A) + Single(B) = 0.0 .\n" +
			"(Single(A) + Single(B)) = 0.0 .\n" +
			"";
		String[] expected = new String[]{
			"1.0 * SINGLE(A) + 1.0 * SINGLE(B) = 0.0 .",
			"1.0 * SINGLE(A) + 1.0 * SINGLE(B) = 0.0 ."
		};

		PSLTest.assertModel(dataStore, input, expected);
	}

	@Test
	public void testArithmeticDivideByZero() {
		String[] input = new String[]{
			"Single(A) / 0 + Single(B) = 0.0 .",
			"2 / 0 * Single(A) + Single(B) = 0.0 .",
			"2 / (2 - 2) * Single(A) + Single(B) = 0.0 .",
			"Single(A) / (-1 + 1) + Single(B) = 0.0 .",
			"Single(A) / @Min[0, 1] + Single(B) = 0.0 ."
		};

		for (String rule : input) {
			try {
				PSLTest.assertRule(dataStore, rule, "");
				fail("Divide by zero did not throw exception.");
			} catch (RuntimeException ex) {
				if (!(ex.getCause() instanceof ArithmeticException)) {
					fail("Divide by zero thre a non-Arithmetic exception.");
				}
			}
		}
	}
}
