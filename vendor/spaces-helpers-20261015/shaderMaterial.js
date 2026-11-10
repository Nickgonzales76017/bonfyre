import * as THREE from '../three-0.180.0/build/three.module.js';

function shaderMaterial(uniforms, vertexShader, fragmentShader, onInit) {
  const entries = Object.entries(uniforms);
  class Material extends THREE.ShaderMaterial {
    static key = THREE.MathUtils.generateUUID();
    constructor(parameters) {
      super({
        uniforms: entries.reduce((acc, [name, value]) => {
          const uniform = THREE.UniformsUtils.clone({
            [name]: {
              value
            }
          });
          return {
            ...acc,
            ...uniform
          };
        }, {}),
        vertexShader,
        fragmentShader
      });
      for (const [name] of entries) {
        Object.defineProperty(this, name, {
          get: () => this.uniforms[name].value,
          set: v => this.uniforms[name].value = v
        });
      }
      Object.assign(this, parameters);
      onInit == null || onInit(this);
    }
  }
  return Material;
}

export { shaderMaterial };
