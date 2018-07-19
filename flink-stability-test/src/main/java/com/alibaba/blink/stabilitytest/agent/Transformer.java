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

import org.jboss.byteman.agent.*;
import org.jboss.byteman.modules.ModuleSystem;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.util.List;

/**
 * byte code transformer used to introduce byteman events into JBoss code
 */
public final class Transformer extends org.jboss.byteman.agent.Transformer {
	/**
	 * constructor allowing this transformer to be provided with access to the JVM's instrumentation
	 * implementation
	 *
	 * @param inst the instrumentation object used to interface to the JVM
	 * @param scriptPaths list of file paths for each input script
	 * @param scriptTexts the text of each input script
	 * @param isRedefine true if class redefinition is allowed false if not
	 * @param moduleSystem the module system to use in transformation
	 * @throws Exception if a script is in error
	 */
	public Transformer(Instrumentation inst, ModuleSystem moduleSystem, List<String> scriptPaths, List<String> scriptTexts, boolean isRedefine)
			throws Exception {
		super(inst, moduleSystem, scriptPaths, scriptTexts, isRedefine);
	}

	/**
	 * The routine which actually does the real bytecode transformation. this is public because it needs to be
	 * callable from the type checker script. In normal running the javaagent is the only class which has a handle
	 * on the registered transformer so it is the only one which can reach this point.
	 * @param ruleScript the script
	 * @param loader the loader of the class being injected into
	 * @param className the name of the class being injected into
	 * @param targetClassBytes the current class bytecode
	 * @return the transformed bytecode or NULL if no transform was applied
	 */
	@Override
	public byte[] transform(RuleScript ruleScript, ClassLoader loader, String className, byte[] targetClassBytes) {
		TransformContext transformContext = new TransformContext(this, ruleScript, className, loader, helperManager);

		return transformContext.transform(targetClassBytes);
	}
}
