// tailwind.config.js
export default {
    content: ['./index.html','./src/**/*.{js,ts,jsx,tsx}'],
    theme: {
        extend: {
    //   colors: {
    //     primary: '#2563eb', // custom blue
    //     secondary: '#f59e0b', // custom orange
    //   },
      fontFamily: {
        sans: ['Roboto', 'sans-serif'],
      },
      gridTemplateColumns :{
        '70/30':'70% 28%',
      },
    },
  },
  plugins: [],
}
