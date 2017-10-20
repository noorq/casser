package net.helenus.core;

public class ConflictingUnitOfWorkException extends Exception {

	final UnitOfWork uow;

	ConflictingUnitOfWorkException(UnitOfWork uow) {
		this.uow = uow;
	}
}
