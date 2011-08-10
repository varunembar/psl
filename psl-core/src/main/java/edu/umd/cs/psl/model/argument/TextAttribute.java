/*
 * This file is part of the PSL software.
 * Copyright 2011 University of Maryland
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
package edu.umd.cs.psl.model.argument;

import edu.umd.cs.psl.model.argument.type.ArgumentType;
import edu.umd.cs.psl.model.argument.type.ArgumentTypes;

/**
 * A particular domain attribute for strings.
 * 
 * @author
 *
 */
public class TextAttribute implements Attribute {

	private final String attribute;
	
	/**
	 * Constructs a textual attribute, given a string.
	 * 
	 * @param a A string value
	 */
	public TextAttribute(String a) {
		attribute = a;
	}
	
	/**
	 * Returns the string value.
	 * 
	 * @return The string value, with a maximum length 30
	 */
	@Override
	public String toString() {
		return "'" + attribute.substring(0, Math.min(attribute.length(), 30)) + "'";
	}
	
	/**
	 * Returns the string value.
	 * 
	 * @return The string value
	 */
	@Override
	public String getAttribute() {
		return attribute;
	}
	
	/**
	 * Returns true, as an the attribute is ground.
	 * 
	 * @return true
	 */
	@Override
	public boolean isGround() {
		return true;
	}
	
	/**
	 * Returns the argument type.
	 * 
	 * @return The argument type
	 */
	@Override
	public ArgumentType getType() {
		return ArgumentTypes.Text;
	}
	
	/**
	 * Returns the hash code.
	 * 
	 * @return The integer hash code
	 */
	@Override
	public int hashCode() {
		return attribute.hashCode();
	}
	
	/**
	 * Determines equality with another object.
	 * 
	 * @return true if equal; false otherwise
	 */
	@Override
	public boolean equals(Object oth) {
		if (oth==this) return true;
		if (oth==null || !(getClass().isInstance(oth)) ) return false;
		return attribute.equals(((TextAttribute)oth).attribute);  
	}
	
}
