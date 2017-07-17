package net.helenus.core;

import java.util.ArrayList;

/**
 * Encapsulates the concept of a "transaction" as a unit-of-work.
 */
public class UnitOfWork {

	private final HelenusSession session;
	private ArrayList<UnitOfWork> nested;

	UnitOfWork(HelenusSession session) {
		this.session = session;
		// log.record(txn::start)
	}

	/**
	 * Marks the beginning of a transactional section of work. Will write a record
	 * to the shared write-ahead log.
	 *
	 * @return the handle used to commit or abort the work.
	 */
	public UnitOfWork begin() {
		if (nested == null) {
			nested = new ArrayList<UnitOfWork>();
		}
		UnitOfWork unitOfWork = new UnitOfWork(session);
		nested.add(unitOfWork);
		return unitOfWork;
	}

	/**
	 * Checks to see if the work performed between calling begin and now can be
	 * committed or not.
	 *
	 * @throws ConflictingUnitOfWorkException
	 *             when the work overlaps with other concurrent writers.
	 */
	public void commit() throws ConflictingUnitOfWorkException {
		// nested.foreach.commit()
		// log.record(txn::provisionalCommit)
		// examine log for conflicts in read-set and write-set between begin and
		// provisional commit
		// if (conflict) { throw new ConflictingUnitOfWorkException(this) }
	}

	/**
	 * Explicitly discard the work and mark it as as such in the log.
	 */
	public void abort() {
		// log.record(txn::abort)
		// cache.invalidateSince(txn::start time)
	}

	public String describeConflicts() {
		return "it's complex...";
	}

}
