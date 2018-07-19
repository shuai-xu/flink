/*
* JBoss, Home of Professional Open Source
* Copyright 2008-10, Red Hat and individual contributors
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*
*/

package com.alibaba.blink.stabilitytest.agent.rule;


import org.jboss.byteman.agent.HelperManager;
import org.jboss.byteman.agent.RuleScript;
import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.exception.CompileException;
import org.jboss.byteman.rule.exception.EarlyReturnException;
import org.jboss.byteman.rule.exception.ExecuteException;
import org.jboss.byteman.rule.exception.ParseException;
import org.jboss.byteman.rule.exception.ThrowException;
import org.jboss.byteman.rule.exception.TypeException;
import org.jboss.byteman.rule.helper.Helper;
import org.jboss.byteman.rule.helper.HelperAdapter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * A rule ties together an event, condition and action. It also maintains a TypeGroup
 * identifying type information derived from these components.
 */
public final class RuleImpl extends org.jboss.byteman.rule.RuleImpl {
	protected RuleImpl(RuleScript ruleScript, ClassLoader targetLoader, HelperManager helperManager)
			throws ParseException, TypeException, CompileException {
		super(ruleScript, targetLoader, helperManager);
	}

	@Override
	protected void executeInternal(Object recipient, Object[] args) throws ExecuteException {
		// type check and createHelperAdapter the rule now if it has not already been done
		if (ensureTypeCheckedCompiled()) {
			Class<?> helperImplementationClass = getHelperImplementationClass();

			// create a helper and get it to execute the rule
			// eventually we will create a subclass of helper for each rule and createHelperAdapter
			// an implementation of execute from the rule source. for now we create a generic
			// helper and call the generic execute method which interprets the rule
			HelperAdapter helper;
			try {
				Constructor constructor = helperImplementationClass.getConstructor(Rule.class);
				helper = (HelperAdapter) constructor.newInstance(this);
				//helper = (RuleHelper)helperClass.newInstance();
				//helper.setRule(this);
				helper.execute(recipient, args);
			} catch (NoSuchMethodException e) {
				// should not happen!!!
				Helper.err("cannot find constructor " + helperImplementationClass.getCanonicalName() + "(Rule) for helper class");
				Helper.errTraceException(e);
				return;
			} catch (InvocationTargetException e) {
				Helper.errTraceException(e);  //To change body of catch statement use File | Settings | File Templates.
			} catch (InstantiationException e) {
				// should not happen
				Helper.err("cannot create instance of " + helperImplementationClass.getCanonicalName());
				Helper.errTraceException(e);
				return;
			} catch (IllegalAccessException e) {
				// should not happen
				Helper.err("cannot access " + helperImplementationClass.getCanonicalName());
				Helper.errTraceException(e);
				return;
			} catch (ClassCastException e) {
				// should not happen
				Helper.err("cast exception " + helperImplementationClass.getCanonicalName());
				Helper.errTraceException(e);
				return;
			} catch (EarlyReturnException e) {
				throw e;
			} catch (ThrowException e) {
				Throwable throwable = e.getThrowable();
				if (throwable != null && throwable instanceof RuntimeException) {
					throw (RuntimeException)throwable;
				}
				throw e;
			} catch (ExecuteException e) {
				Helper.err(getName() + " : " + e);
				throw e;
			} catch (Throwable throwable) {
				Helper.err(getName() + " : " + throwable);
				throw new ExecuteException(getName() + "  : caught " + throwable, throwable);
			}
		}
	}
}
