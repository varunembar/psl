/*
 * This file is part of the PSL software.
 * Copyright 2011-2015 University of Maryland
 * Copyright 2013-2018 The Regents of the University of California
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
package org.linqs.psl.database;

import org.linqs.psl.model.atom.AtomCache;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.atom.ObservedAtom;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.model.formula.Formula;
import org.linqs.psl.model.predicate.Predicate;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.term.Constant;
import org.linqs.psl.model.term.Variable;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A data model for retrieving and persisting {@link GroundAtom GroundAtoms}.
 *
 * Every GroundAtom retrieved from a Database is either a {@link RandomVariableAtom}
 * or an {@link ObservedAtom}. The method {@link #getAtom(Predicate, Constant...)}
 * determines which type a GroundAtom is. In addition, a GroundAtom with a
 * {@link StandardPredicate} can be persisted in a Database. If a
 * GroundAtom is persisted, it is persisted in one of the Partitions the
 * Database can read and is available for querying via {@link #executeQuery(DatabaseQuery)}.
 *
 * <h2>Setup</h2>
 *
 * Databases are instantiated via {@link DataStore#getDatabase} methods.
 *
 * A Database writes to and reads from one {@link Partition} of a DataStore
 * and can read from additional Partitions. The write Partition of a Database
 * may not be a read (or write) Partition of any other Database managed by the datastore.
 *
 * A Database can be instantiated with a set of StandardPredicates
 * to close. (Any StandardPredicate not closed initially remains open.) Whether
 * a StandardPredicate is open or closed affects the behavior of
 * {@link #getAtom(Predicate, Constant...)}.
 *
 * <h2>Retrieving GroundAtoms</h2>
 *
 * A Database is the canonical source for a set of GroundAtoms.
 * GroundAtoms should only be retrieved via {@link #getAtom(Predicate, Constant...)}
 * to ensure there exists only a single object for each GroundAtom from the Database.
 *
 * A Database contains an {@link AtomCache} which is used to store GroundAtoms
 * that have been instantiated in memory and ensure these objects are unique.
 * The AtomCache is accessible via {@link #getAtomCache()}.
 *
 * <h2>Persisting RandomVariableAtoms</h2>
 *
 * A RandomVariableAtom can be persisted (including updated) in the write
 * Partition via {@link #commit(RandomVariableAtom)} or
 * {@link RandomVariableAtom#commitToDB()}.
 *
 * <h2>Querying for Groundings</h2>
 *
 * {@link DatabaseQuery DatabaseQueries} can be run via {@link #executeQuery(DatabaseQuery)}.
 * Note that queries only act on the GroundAtoms persisted in Partitions and
 * GroundAtoms with FunctionalPredicates.
 */
public interface Database {

	/**
	 * Returns the GroundAtom for the given Predicate and GroundTerms.
	 *
	 * Any GroundAtom with a {@link StandardPredicate} can be retrieved if and only
	 * if its Predicate was registered with the DataStore at the time of the Database's
	 * instantiation. Any GroundAtom with an ExternalFunctionalPredicate} can also be retrieved.
	 * This method first checks the {@link AtomCache} to see if the GroundAtom already
	 * exists in memory. If it does, then that object is returned. (The AtomCache is
	 * accessible via {@link #getAtomCache()}.)
	 *
	 * If the GroundAtom does not exist in memory, then it will be instantiated and
	 * stored in the AtomCache before being returned. The subtype and state of the
	 * instantiated GroundAtom depends on several factors:
	 * <ul>
	 *	<li>If the GroundAtom is persisted in a read Partition, then it will be
	 *	instantiated as an {@link ObservedAtom} with the persisted state.</li>
	 *	<li>If the GroundAtom is persisted in the write Partition, then it will be
	 *	instantiated with the persisted state. It will be instantiated as an
	 *	ObservedAtom if its Predicate is closed and as a {@link RandomVariableAtom}
	 *	if it is open.</li>
	 *	<li>If the GroundAtom has a StandardPredicate but is not persisted
	 *	in any of the Database's partitions, it will be instantiated with a truth
	 *	value of 0.0. It will be instantiated as an ObservedAtom if its Predicate
	 *	is closed and as a RandomVariableAtom
	 *	if it is open.</li>
	 *	<li>If the GroundAtom has an ExternalFunctionalPredicate, then it will be
	 *	instantiated as an ObservedAtom with the functionally defined
	 *	truth value.</li>
	 * </ul>
	 *
	 * @param predicate the Predicate of the Atom
	 * @param arguments the GroundTerms of the Atom
	 * @return the Atom
	 * @throws IllegalArgumentException if predicate is not registered or arguments are not valid
	 * @throws IllegalStateException if the Atom is persisted in multiple read Partitions
	 */
	public GroundAtom getAtom(Predicate predicate, Constant... arguments);

	/**
	 * Check to see if a ground atom exists in the database.
	 * This looks for a real ground atom and ignores the closed-world assumption.
	 * If found, the atom will be cached for subsequent requests to this or getAtom().
	 */
	public boolean hasAtom(StandardPredicate predicate, Constant... arguments);

	/**
	 * Get a count of all the ground atoms for a predicate.
	 * By "ground", we mean that it exists in the database.
	 * This will not leverage the closed world assumption for any atoms.
	 *
	 * @param predicate the predicate to get a count for
	 * @return The count of all ground atoms present in any partition of the database.
	 */
	public int countAllGroundAtoms(StandardPredicate predicate);

	/**
	 * Get a count of all the ground RandomVariableAtoms for a predicate.
	 * By "ground", we mean that it exists in the database.
	 * This will not leverage the closed world assumption for any atoms.
	 *
	 * @param predicate the predicate to get a count for
	 * @return The count of all ground atoms present in the write partition of the database.
	 */
	public int countAllGroundRandomVariableAtoms(StandardPredicate predicate);

	/**
	 * Fetch all the ground atoms for a predicate.
	 * By "ground", we mean that it exists in the database.
	 * This will not leverage the closed world assumption for any atoms.
	 *
	 * @param predicate the predicate to fetch atoms for
	 * @return All ground atoms present in any partition of the database.
	 */
	public List<GroundAtom> getAllGroundAtoms(StandardPredicate predicate);

	/**
	 * Fetch all the ground RandomVariableAtoms for a predicate.
	 * By "ground", we mean that it exists in the database.
	 * This will not leverage the closed world assumption for any atoms.
	 *
	 * @param predicate the predicate to fetch atoms for
	 * @return All ground atoms present in the write partition of the database.
	 */
	public List<RandomVariableAtom> getAllGroundRandomVariableAtoms(StandardPredicate predicate);

	/**
	 * Fetch all the ground ObservedAtoms for a predicate.
	 * By "ground", we mean that it exists in the database.
	 * This will not leverage the closed world assumption for any atoms.
	 *
	 * @param predicate the predicate to fetch atoms for
	 * @return All ground atoms present in the write partition of the database.
	 */
	public List<ObservedAtom> getAllGroundObservedAtoms(StandardPredicate predicate);

	/**
	 * Removes the GroundAtom from the Database, if it exists.
	 *
	 *
	 * @param a the GroundAtom to delete
	 * @return If an atom was removed
	 * @throws IllegalArgumentException if predicate is not registered or arguments are not valid
	 */
	public boolean deleteAtom(GroundAtom a);

	/**
	 * Persists a RandomVariableAtom in this Database's write Partition.
	 *
	 * If the RandomVariableAtom has already been persisted in the write Partition,
	 * it will be updated.
	 *
	 * @param atom the Atom to persist
	 * @throws IllegalArgumentException if atom does not belong to this Database
	 */
	public void commit(RandomVariableAtom atom);

	/**
	 * A batch form or commit().
	 * When possible, this commit should be used.
	 */
	public void commit(Collection<RandomVariableAtom> atoms);

	/**
	 * A form of commit() that allows the caller to choose the specific partition
	 * the atoms are comitted to.
	 * Should only be used if you REALLY know what you are doing.
	 */
	public void commit(Collection<RandomVariableAtom> atoms, int partitionId);

	/**
	 * Move all ground atoms of a predicate/partition combination into
	 * the write partition.
	 * Be careful not to call this while the database is in use.
	 */
	public void moveToWritePartition(StandardPredicate predicate, int oldPartitionId);

	/**
	 * Returns all groundings of a Formula that match a DatabaseQuery.
	 *
	 * @param query the query to match
	 * @return a list of lists of substitutions of {@link Constant GroundTerms}
	 *				 for {@link Variable Variables}
	 * @throws IllegalArgumentException if the query Formula is invalid
	 */
	public ResultList executeQuery(DatabaseQuery query);

	/**
	 * Like executeQuery(), but specifically for grounding queries.
	 * This will use extra optimizations.
	 */
	public ResultList executeGroundingQuery(Formula formula);

	/**
	 * Returns whether a StandardPredicate is closed in this Database.
	 *
	 * @param predicate the Predicate to check
	 * @return TRUE if predicate is closed
	 */
	public boolean isClosed(StandardPredicate predicate);

	/**
	 * Returns the set of StandardPredicates registered with this Database.
	 * Note that the result can differ from calling
	 * {@link DataStore#getRegisteredPredicates()} on this Database's backing
	 * DataStore, since additional predicates might have been registered since
	 * this Database was created.
	 *
	 * @return the set of StandardPredicates registered with this Database
	 */
	public Set<StandardPredicate> getRegisteredPredicates();

	/**
	 * @return the DataStore backing this Database
	 */
	public DataStore getDataStore();

	/**
	 * @return the Database's AtomCache
	 */
	public AtomCache getAtomCache();

	/**
	 * Releases the {@link Partition Partitions} used by this Database.
	 */
	public void close();
}
