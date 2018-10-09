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

package com.alibaba.blink.stabilitytest.agent;

import com.alibaba.blink.stabilitytest.agent.rule.DefaultRuleFactory;

import org.jboss.byteman.agent.HelperManager;
import org.jboss.byteman.agent.RuleScript;
import org.jboss.byteman.agent.Transformer;

/**
 * Class used to localise the context information employed when creating a rule from a rule script and
 * using it to transform a method
 */
public final class TransformContext extends org.jboss.byteman.agent.TransformContext {

	public TransformContext(Transformer transformer, RuleScript ruleScript, String triggerClassName, ClassLoader loader, HelperManager helperManager) {
		super(transformer, ruleScript, triggerClassName, loader, helperManager, new DefaultRuleFactory());
	}

}
